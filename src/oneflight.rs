//! An async caching layer.
//!
//! Evaluates an async function f for a given key if it's not already in the cache.
//!
//! Ensures that the function is only evaluated once per key, even if multiple requests are in flight.
//! Failed (or cancelled) evaluations are not cached, and leave no entry behind.

use std::{collections::HashMap, hash::Hash, sync::Arc};

use futures::lock::Mutex;
use std::sync::Mutex as SyncMutex;

type Slot<V> = Arc<Mutex<Option<Arc<V>>>>;

pub struct OneFlightCache<K, V> {
    cache: SyncMutex<HashMap<K, Slot<V>>>,
}

impl<K, V> Default for OneFlightCache<K, V>
where
    K: Hash + Eq + Clone,
    V: Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
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

    /// Number of entries, including loads that are still in flight.
    pub fn len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drops every cached entry. Loads already in flight still return their
    /// value to their callers, but the value is not kept in the cache.
    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }

    pub async fn get<F, E>(&self, key: &K, f: F) -> Result<Arc<V>, E>
    where
        F: AsyncFnOnce(&K) -> Result<V, E>,
    {
        loop {
            let slot = {
                // Acquire the global lock to ensure only one slot is inserted per key.
                let mut cache = self.cache.lock().unwrap();
                cache.entry(key.clone()).or_default().clone()
            };
            // Acquire the slot lock to ensure the future is only executed once.
            let mut guard = slot.lock().await;
            if let Some(v) = &*guard {
                return Ok(v.clone());
            }
            // An empty slot that is no longer in the map was abandoned by a
            // failed load or by clear(). Start over with the current slot.
            if !self.is_current(key, &slot) {
                continue;
            }
            // Declared after `guard` so it drops first: on failure or
            // cancellation the slot leaves the map while we still hold its
            // lock, so no waiter can start loading into an abandoned slot.
            let mut cleanup = RemoveOnDrop {
                cache: &self.cache,
                key,
                slot: &slot,
                armed: true,
            };
            let v = Arc::new(f(key).await?);
            *guard = Some(v.clone());
            cleanup.armed = false;
            return Ok(v);
        }
    }

    fn is_current(&self, key: &K, slot: &Slot<V>) -> bool {
        let cache = self.cache.lock().unwrap();
        cache.get(key).is_some_and(|s| Arc::ptr_eq(s, slot))
    }
}

/// Removes `slot` from the map on drop, unless disarmed.
struct RemoveOnDrop<'a, K: Hash + Eq, V> {
    cache: &'a SyncMutex<HashMap<K, Slot<V>>>,
    key: &'a K,
    slot: &'a Slot<V>,
    armed: bool,
}

impl<K: Hash + Eq, V> Drop for RemoveOnDrop<'_, K, V> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Don't panic while possibly unwinding; a poisoned map is still usable.
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if cache.get(self.key).is_some_and(|s| Arc::ptr_eq(s, self.slot)) {
            cache.remove(self.key);
        }
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
    fn failure_leaves_no_entry() {
        block_on(async {
            let cache = OneFlightCache::<u32, u32>::new();
            let r = cache.get(&1, async |_| Err::<u32, _>("boom")).await;
            assert_eq!(r, Err("boom"));
            assert!(cache.is_empty());

            // A later call retries and caches the value.
            let r = cache.get(&1, async |k| Ok::<_, ()>(k + 1)).await;
            assert_eq!(*r.unwrap(), 2);
            assert_eq!(cache.len(), 1);
        })
    }

    #[test]
    fn cancelled_load_leaves_no_entry() {
        let cache = OneFlightCache::<u32, u32>::new();
        {
            let fut = cache.get(&1, async |_| {
                futures::future::pending::<()>().await;
                Ok::<u32, ()>(0)
            });
            let mut fut = Box::pin(fut);
            // Start the load, then drop it mid-flight.
            assert!(futures::FutureExt::now_or_never(&mut fut).is_none());
            assert_eq!(cache.len(), 1);
        }
        assert!(cache.is_empty());
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
