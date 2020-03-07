use super::{OrderBook, ProcessorAction};
use crate::codegen::{Level, Summary};
use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::sink::SinkExt;
use futures::stream::{self, SelectAll as StreamSelector, StreamExt};
use ordered_float::NotNan;
use seahash::SeaHasher;
use uuid::Uuid;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Number of levels in order book summary.
const NUM_ITEMS: usize = 10;

/// Merger takes care of registering new client, gathering the order books
/// from processors, generating a summary and fanning out the summary across clients.
pub struct Merger {
    senders: Vec<UnboundedSender<ProcessorAction>>,
    client_sender: UnboundedSender<(Uuid, String, UnboundedSender<Summary>)>,
}

enum MergerTask {
    Update((String, Summary)),
    IncomingClient((Uuid, String, UnboundedSender<Summary>)),
}

impl Merger {
    /// Create a new instance and spawn tasks in the background.
    pub fn start(
        senders: Vec<UnboundedSender<ProcessorAction>>,
        mut book_receiver: StreamSelector<UnboundedReceiver<OrderBook>>,
    ) -> Self {
        // Set buffer size for fanout receiver - for throttling as gRPC clients increase.
        let (mut fanout_tx, fanout_rx) = mpsc::channel(10);
        // Unbounded because client subscription will be fast.
        let (client_tx, client_rx) = mpsc::unbounded();

        tokio::spawn(async move {
            debug!("Spawning task for collecting order book summary.");
            let mut summary_map: HashMap<String, Summary> = HashMap::new();

            while let Some(book) = book_receiver.next().await {
                let symbol = book.symbol.clone();
                let summary = summary_map
                    .entry(symbol.clone())
                    .or_insert_with(Summary::default);
                let has_changed = summary.update_from_book(book);
                if has_changed {
                    let _ = fanout_tx.send((symbol, summary.clone())).await;
                } else {
                    trace!("Skipping fanout message because summary is same.");
                }
            }
        });

        tokio::spawn(async move {
            debug!("Spawning task for client fanout.");
            // NOTE: Eventually, when we reach a point where we have to scale
            // for a number of gRPC clients, we'll have to spawn a new task for clients
            // based on symbols.
            let mut clients: HashMap<String, Vec<_>> = HashMap::new();
            let mut rx = stream::select(
                client_rx.map(MergerTask::IncomingClient),
                fanout_rx.map(MergerTask::Update),
            );

            loop {
                match rx.next().await {
                    Some(MergerTask::Update((symbol, data))) => {
                        let c = clients.get_mut(&symbol).expect("symbol doesn't exist?");
                        *c = c
                            .drain(..)
                            .filter_map(|(id, sender): (Uuid, UnboundedSender<Summary>)| {
                                // Receiver is dropped, remove client.
                                if let Err(_) = sender.unbounded_send(data.clone()) {
                                    info!("Removing subscription for client (ID: {})", id);
                                    return None;
                                }

                                Some((id, sender))
                            })
                            .collect();
                    }
                    Some(MergerTask::IncomingClient((id, symbol, tx))) => {
                        debug!("Client (ID: {}) is now subscribed.", id);
                        clients
                            .entry(symbol)
                            .or_insert_with(Vec::new)
                            .push((id, tx));
                    }
                    _ => (),
                }
            }
        });

        Merger {
            senders,
            client_sender: client_tx,
        }
    }

    /// Registers an incoming client with an ID and the symbol they've subscribed to,
    /// and returns a stream with order book summary for that symbol.
    pub async fn add_client(&self, client_id: Uuid, symbol: String) -> UnboundedReceiver<Summary> {
        let symbol = symbol.to_lowercase();
        for tx in &self.senders {
            let _ = tx.unbounded_send(ProcessorAction::Subscribe {
                symbol: symbol.clone(),
                replace: false,
            });
        }

        let (tx, rx) = mpsc::unbounded();
        let _ = self.client_sender.unbounded_send((client_id, symbol, tx));
        rx
    }
}

impl Summary {
    /// Updates existing data from the provided book, returning if this book
    /// has changed the summary.
    fn update_from_book(&mut self, book: OrderBook) -> bool {
        let bids = book
            .bids
            .iter()
            .map(|&(price, amount)| Level {
                exchange: book.exchange.into(),
                price,
                amount,
            })
            .collect();

        let bids_changed = Self::merge_sorted(book.exchange, &mut self.bids, bids, |l1, l2| {
            l2.partial_cmp(l1).unwrap()
        });

        let asks = book
            .asks
            .iter()
            .map(|&(price, amount)| Level {
                exchange: book.exchange.into(),
                price,
                amount,
            })
            .collect();

        let asks_changed = Self::merge_sorted(book.exchange, &mut self.asks, asks, |l1, l2| {
            l1.partial_cmp(l2).unwrap()
        });

        self.spread = self.asks.get(0).map(|l| l.price).unwrap_or(0.0)
            - self.bids.get(0).map(|l| l.price).unwrap_or(0.0);

        asks_changed | bids_changed
    }

    /// Assumes that the `existing` vector is from multiple exchanges and `appendable`
    /// is from one order book (i.e., one exchange), removes the levels matching the given
    /// exchange, merges the `appendable` vector, sorts and truncates so that it matches
    /// the latest summary on 10 asks and bids.
    // NOTE: We're sticking with vectors (and not say, linked lists), because
    // with modern processors, this goes easy on cache lines and sorting is quite
    // quick for 10 elements.
    fn merge_sorted<F>(
        exchange: &str,
        existing: &mut Vec<Level>,
        mut appendable: Vec<Level>,
        compare: F,
    ) -> bool
    where
        F: FnMut(&Level, &Level) -> Ordering,
    {
        if appendable.is_empty() {
            return false;
        }

        if existing.is_empty() {
            *existing = appendable;
            return true;
        }

        // TODO: A better way than hashing would be to implement an algorithm which
        // could do this really fast (given that the vectors are already sorted),
        // but this is easier and it doesn't affect the performance as of now.

        let mut hasher = SeaHasher::new();
        existing.hash(&mut hasher);
        let hash_1 = hasher.finish();

        // Truncate unnecessary items since they're already sorted.
        existing.retain(|l| l.exchange != exchange);
        appendable.truncate(NUM_ITEMS);
        existing.append(&mut appendable);
        existing.sort_by(compare);
        existing.truncate(NUM_ITEMS);

        hasher = SeaHasher::new();
        existing.hash(&mut hasher);
        let hash_2 = hasher.finish();

        hash_1 != hash_2
    }
}

/// **NOTE:** This implementation assumes that we're comparing levels for
/// the **same exchanges** and that the amount and price is not `NaN`.
impl PartialOrd for Level {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.price.partial_cmp(&other.price).unwrap() {
            Ordering::Equal => other.amount.partial_cmp(&self.amount),
            ord => Some(ord),
        }
    }
}

impl Hash for Level {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        // We've already filtered NaN.
        NotNan::new(self.price).unwrap().hash(state);
        NotNan::new(self.amount).unwrap().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::super::OrderBook;
    use crate::codegen::Summary;

    // TODO: This has 4 tests. Split this into separate tests.
    #[test]
    fn test_book_sorting() {
        let mut summary = Summary::default();
        // Initial order book only adds stuff.
        let has_changed = summary.update_from_book(OrderBook {
            exchange: "e1",
            symbol: "s1".into(),
            // NOTE: Bids and asks will be sorted when they reach the merger.
            bids: vec![(2.739, 25.74), (2.56, 0.29), (2.45, 3.52)],
            asks: vec![(2.8, 74.95), (2.83, 0.56), (2.835, 125.89)],
        });

        assert!(has_changed);
        assert!(summary.spread > 0.06 && summary.spread < 0.061);
        assert_eq!(summary.asks.len(), 3);
        assert_eq!(summary.bids.len(), 3);

        // Add book from another exchange.
        let has_changed = summary.update_from_book(OrderBook {
            exchange: "e2",
            symbol: "s2".into(),
            bids: vec![(2.565, 17.3), (2.39, 0.05)],
            asks: vec![(2.91, 0.573), (2.7953, 57.41)],
        });

        assert!(has_changed);
        // Both have been merged.
        assert_eq!(
            summary
                .bids
                .iter()
                .map(|l| (l.price, l.amount))
                .collect::<Vec<_>>(),
            vec![
                (2.739, 25.74),
                (2.565, 17.3),
                (2.56, 0.29),
                (2.45, 3.52),
                (2.39, 0.05),
            ]
        );
        assert_eq!(
            summary
                .asks
                .iter()
                .map(|l| (l.price, l.amount))
                .collect::<Vec<_>>(),
            vec![
                (2.7953, 57.41),
                (2.8, 74.95),
                (2.83, 0.56),
                (2.835, 125.89),
                (2.91, 0.573),
            ]
        );
        assert!(summary.spread > 0.0563 && summary.spread < 0.0564);

        for i in 0..2 {
            // Add book from existing exchange.
            let has_changed = summary.update_from_book(OrderBook {
                exchange: "e2",
                symbol: "s2".into(),
                bids: vec![(2.479, 99.1), (2.759, 0.95)],
                asks: vec![(2.91, 0.573), (2.8, 0.05)],
            });

            if i == 0 {
                assert!(has_changed);
            } else {
                // It shouldn't have changed the second time.
                assert!(!has_changed);
            }

            // Both have been merged.
            assert_eq!(
                summary
                    .bids
                    .iter()
                    .map(|l| (l.price, l.amount))
                    .collect::<Vec<_>>(),
                vec![
                    (2.759, 0.95),
                    (2.739, 25.74),
                    (2.56, 0.29),
                    (2.479, 99.1),
                    (2.45, 3.52),
                ]
            );
            assert_eq!(
                summary
                    .asks
                    .iter()
                    .map(|l| (l.price, l.amount))
                    .collect::<Vec<_>>(),
                vec![
                    (2.8, 74.95),
                    (2.8, 0.05),
                    (2.83, 0.56),
                    (2.835, 125.89),
                    (2.91, 0.573),
                ]
            );
            assert!(summary.spread > 0.040999 && summary.spread < 0.041);
        }
    }
}
