use crate::codegen::{Level, Summary};
use super::{OrderBook, ProviderAction};
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::stream::{self, Stream, StreamExt, SelectAll as StreamSelector};
use futures::lock::Mutex;

use std::collections::HashMap;
use std::iter;
use std::sync::Arc;

pub struct Merger {
    senders: Vec<UnboundedSender<ProviderAction>>,
    summary: Arc<Mutex<HashMap<String, Arc<Mutex<Summary>>>>>,
}

impl Merger {
    pub fn new(senders: Vec<UnboundedSender<ProviderAction>>) -> Self {
        Merger {
            senders,
            summary: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn add_client(&self, symbol: String) -> impl Stream<Item=Summary> {
        for tx in &self.senders {
            let _ = tx.unbounded_send(ProviderAction::Subscribe { symbol: symbol.clone(), replace: false });
        }

        stream::iter(iter::once(Summary::default()))
    }

    pub fn start(&self, book_receiver: StreamSelector<UnboundedReceiver<OrderBook>>) {
        let global = self.summary.clone();
        let mut rx = book_receiver;
        tokio::spawn(async move {
            while let Some(book) = rx.next().await {
                let s = {
                    let mut symbols = global.lock().await;
                    match symbols.get(&book.symbol) {
                        Some(a) => a.clone(),
                        None => {
                            let summ = Arc::new(Mutex::new(Summary::default()));
                            symbols.insert(book.symbol, summ.clone());
                            summ
                        },
                    }
                };

                let summary = s.lock().await;
                // TODO: Sort books and notify.
            }
        });
    }
}
