use crate::errors;
use bytes::Bytes;
use crossbeam_channel::{select, Sender};
use log::{error, info};
use std::{thread, fmt::Display};

pub struct BgWorker<B>
where
    B: Send + 'static,
{
    name: String,
    work_sender: Sender<B>,
    stop_sender: Sender<bool>,
}

impl<B> BgWorker<B>
where
    B: Send + 'static,
{
    pub fn new(
        name: &str,
        work: impl Fn(B) -> Result<Bytes, errors::DbError> + Send + 'static,
    ) -> Self {
        let (s, r) = crossbeam_channel::unbounded::<B>();
        let (stop_s, stop_r) = crossbeam_channel::bounded::<bool>(1);
        let worker = BgWorker {
            name: name.to_owned(),
            work_sender: s,
            stop_sender: stop_s,
        };
        let worker_name = name.to_owned();
        thread::spawn(move || loop {
            select! {
                recv(r) -> task => {
                    match task {
                        Ok(task) => {
                            match work(task) {
                                Err(err) => error!("background worker {} process data with error {:?}", &worker_name, err),
                                Ok(msg) => println!("{:#?}", msg)
                            }
                        }
                        Err(err) => {
                            error!("background worker {} recv data with error {:?}", &worker_name, err);
                        }
                    }
                },
                recv(stop_r) -> _ => {
                    info!("background worker {} Received stop signal, worker exit", &worker_name);
                    break;
                },
            }
        });
        worker
    }

    pub fn send(&self, task: B) {
        self.work_sender.send(task).unwrap();
    }

    pub fn stop(&self) {
        self.stop_sender.send(true).unwrap();
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;

    use crate::data::{entry::Entry, meta::Meta};

    use super::BgWorker;
    use log::{info, error, warn};

    #[test]
    fn test_bg_worker() {
        let _ = env_logger::builder().filter_level(log::LevelFilter::Info).is_test(true).try_init();
        let worker = BgWorker::new("test", |record: Entry| {
            Ok(record.key)
        });
        worker.send(Entry {
            key: Bytes::from("key1"),
            value: Bytes::from("value"),
            meta: Meta::default(),
            crc: 1,
        });
        worker.send(Entry {
            key: Bytes::from("key2"),
            value: Bytes::from("value"),
            meta: Meta::default(),
            crc: 1,
        });
        std::thread::sleep(std::time::Duration::from_secs(1));
        worker.stop();
    }
}