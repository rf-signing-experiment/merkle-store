//! Async function cache.
//! 
//! Ensures that only one execution of a function is in flight for a given key at
//! a time.

use std::{collections::HashMap, hash::Hash, sync::Arc};

use futures::lock::Mutex;
use std::sync::Mutex as SyncMutex;

type Slot<V> = Arc<Mutex<Option<Arc<V>>>>;

#[derive(Default)]
pub struct OneFlightCache<K, V> {
    cache: SyncMutex<HashMap<K, Slot<V>>>,
}

impl<K, V> OneFlightCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Send + Sync,
{
    pub fn new() -> Self {
        Self {
            cache: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }

    pub async fn get<F, E>(&self, key: &K, f: F) -> Result<Arc<V>, E>
    where
        F: AsyncFnOnce(&K) -> Result<V, E>,
    {
        let slot = {
            // Ensure only one slot created per key.
            let mut cache = self.cache.lock().unwrap();
            cache.entry(key.clone()).or_default().clone()
        };
        // Ensure only one execution.
        let mut guard = slot.lock().await;
        if let Some(v) = &*guard {
            return Ok(v.clone());
        }
        // We now hold the lock, so run the function and store the result.
        let v = Arc::new(f(key).await?);
        *guard = Some(v.clone());
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Barrier,
            atomic::{AtomicU32, Ordering},
        },
        thread,
        time::Duration,
    };

    use futures::executor::block_on;

    use crate::oneflight::OneFlightCache;

    struct Incrementer {
        i: AtomicU32,
        of: OneFlightCache<u32, u32>,
    }

    impl Incrementer {
        async fn inc_uncached(&self, k: &u32) -> Result<u32, ()> {
            self.i.fetch_add(1, Ordering::AcqRel);
            Ok(k + 1)
        }

        fn new() -> Self {
            Self {
                i: Default::default(),
                of: OneFlightCache::new(),
            }
        }

        async fn inc(&self, k: &u32) -> Result<Arc<u32>, ()> {
            self.of.get(k, async |k| self.inc_uncached(k).await).await
        }
    }

    #[test]
    fn basic() {
        block_on(async {
            let i = Incrementer::new();

            assert_eq!(*i.inc(&0).await.unwrap(), 1);
            assert_eq!(*i.inc(&1).await.unwrap(), 2);
            assert_eq!(*i.inc(&0).await.unwrap(), 1);
            assert_eq!(i.i.load(Ordering::Acquire), 2);
        })
    }

    #[test]
    fn failure_is_not_cached() {
        block_on(async {
            let cache = OneFlightCache::<u32, u32>::new();
            let r = cache.get(&1, async |_| Err::<u32, _>("boom")).await;
            assert_eq!(r, Err("boom"));

            // A later call retries and caches the value.
            let r = cache.get(&1, async |k| Ok::<_, ()>(k + 1)).await;
            assert_eq!(*r.unwrap(), 2);
            assert_eq!(cache.len(), 1);
        })
    }

    #[test]
    fn cancelled_load_is_retried() {
        let cache = OneFlightCache::<u32, u32>::new();
        {
            let fut = cache.get(&1, async |_| {
                futures::future::pending::<()>().await;
                Ok::<u32, ()>(0)
            });
            let mut fut = Box::pin(fut);
            // Start the load, then drop it mid-flight.
            assert!(futures::FutureExt::now_or_never(&mut fut).is_none());
        }
        let r = block_on(cache.get(&1, async |k| Ok::<_, ()>(k + 1)));
        assert_eq!(*r.unwrap(), 2);
    }

    #[test]
    fn clear_empties_cache() {
        block_on(async {
            let i = Incrementer::new();
            i.inc(&0).await.unwrap();
            i.inc(&1).await.unwrap();
            assert_eq!(i.of.len(), 2);

            i.of.clear();
            assert!(i.of.is_empty());

            // Values are reloaded after a clear.
            assert_eq!(*i.inc(&0).await.unwrap(), 1);
            assert_eq!(i.i.load(Ordering::Acquire), 3);
        })
    }

    const THREADS: usize = 8;

    #[test]
    fn concurrent_requests_load_once() {
        let cache = OneFlightCache::<u32, u32>::new();
        let loads = AtomicU32::new(0);
        let barrier = Barrier::new(THREADS);
        thread::scope(|s| {
            for _ in 0..THREADS {
                s.spawn(|| {
                    barrier.wait();
                    let v = block_on(cache.get(&7, async |k| {
                        loads.fetch_add(1, Ordering::AcqRel);
                        thread::sleep(Duration::from_millis(50));
                        Ok::<_, ()>(k * 2)
                    }));
                    assert_eq!(*v.unwrap(), 14);
                });
            }
        });
        assert_eq!(loads.load(Ordering::Acquire), 1);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn waiters_retry_after_failure() {
        let cache = OneFlightCache::<u32, u32>::new();
        let loads = AtomicU32::new(0);
        let barrier = Barrier::new(THREADS);
        let results: Vec<_> = thread::scope(|s| {
            let handles: Vec<_> = (0..THREADS)
                .map(|_| {
                    s.spawn(|| {
                        barrier.wait();
                        block_on(cache.get(&7, async |k| {
                            let n = loads.fetch_add(1, Ordering::AcqRel);
                            thread::sleep(Duration::from_millis(50));
                            // Only the first load fails.
                            if n == 0 { Err(()) } else { Ok(k * 2) }
                        }))
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let failures = results.iter().filter(|r| r.is_err()).count();
        assert_eq!(failures, 1);
        assert!(results.iter().flatten().all(|v| **v == 14));
        assert_eq!(loads.load(Ordering::Acquire), 2);
        assert_eq!(cache.len(), 1);
    }
}
