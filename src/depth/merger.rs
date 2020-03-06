use super::{OrderBook, ProcessorAction};
use crate::codegen::{Level, Summary};
use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::sink::SinkExt;
use futures::stream::{self, SelectAll as StreamSelector, StreamExt};
use uuid::Uuid;

use std::cmp::Ordering;
use std::collections::HashMap;

const NUM_ITEMS: usize = 10;

pub struct Merger {
    senders: Vec<UnboundedSender<ProcessorAction>>,
    client_sender: UnboundedSender<(Uuid, UnboundedSender<Summary>)>,
}

enum MergerTask {
    Update(Summary),
    IncomingClient((Uuid, UnboundedSender<Summary>)),
}

impl Merger {
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
                let summary = summary_map
                    .entry(book.symbol.clone())
                    .or_insert_with(Summary::default);
                let has_changed = summary.update_from_book(book);
                if has_changed {
                    let _ = fanout_tx.send(summary.clone()).await;
                }
            }
        });

        tokio::spawn(async move {
            debug!("Spawning task for client fanout.");
            let mut clients = vec![];
            let mut rx = stream::select(
                client_rx.map(MergerTask::IncomingClient),
                fanout_rx.map(MergerTask::Update),
            );

            loop {
                match rx.next().await {
                    Some(MergerTask::Update(data)) => {
                        clients = clients
                            .into_iter()
                            .filter_map(|(id, sender): (Uuid, UnboundedSender<Summary>)| {
                                if let Err(_) = sender.unbounded_send(data.clone()) {
                                    info!("Removing subscription for client (ID: {})", id);
                                    return None;
                                }

                                Some((id, sender))
                            })
                            .collect();
                    }
                    Some(MergerTask::IncomingClient(client)) => {
                        debug!("Client (ID: {}) is now subscribed.", client.0);
                        clients.push(client);
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

    pub async fn add_client(&self, client_id: Uuid, symbol: String) -> UnboundedReceiver<Summary> {
        for tx in &self.senders {
            let _ = tx.unbounded_send(ProcessorAction::Subscribe {
                symbol: symbol.clone(),
                replace: false,
            });
        }

        let (tx, rx) = mpsc::unbounded();
        let _ = self.client_sender.unbounded_send((client_id, tx));
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

        Self::merge_sorted(&mut self.bids, bids, |l1, l2| {
            match l2.price.partial_cmp(&l1.price).unwrap() {
                Ordering::Equal => l2.amount.partial_cmp(&l1.amount).unwrap(),
                ord => ord,
            }
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

        Self::merge_sorted(&mut self.asks, asks, |l1, l2| {
            match l1.price.partial_cmp(&l2.price).unwrap() {
                Ordering::Equal => l2.amount.partial_cmp(&l1.amount).unwrap(),
                ord => ord,
            }
        });

        // We could implement/use a sorting algorithm which can update the
        // asks/bids and tell us if the book has been updated, but this is
        // easier and it doesn't affect the performance as of now.
        true
    }

    /// Appends second vector to the first, sorts and returns a truncated vector.
    // Note that we're sticking with vectors (and not say, linked lists), because
    // with modern processors, this goes easy on cache lines and sorting is quite
    // quick for 10 elements.
    fn merge_sorted<T, F>(v1: &mut Vec<T>, mut v2: Vec<T>, compare: F)
    where
        F: FnMut(&T, &T) -> Ordering,
    {
        if v2.is_empty() {
            return;
        }

        if v1.is_empty() {
            *v1 = v2;
            return;
        }

        v1.append(&mut v2);
        // TODO: We don't have to sort the whole thing. We could implement an
        // algorithm which could do this really fast (given that the vectors
        // are already sorted).
        v1.sort_by(compare);
        v1.truncate(NUM_ITEMS);
    }
}
