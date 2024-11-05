use std::collections::{HashMap, HashSet};
use std::hash::Hash;

#[derive(Debug)]
struct Info<D, I> {
    data: D,
    ids: HashSet<I>,
}

#[derive(Debug)]
#[allow(dead_code)]
/// A map that maps a key to a data and a set of ids.
/// The data is the value associated with the key.
/// The ids are the values associated with the key.
pub(crate) struct KeyToDataAndIdMap<K, D, I> {
    inner_map: HashMap<K, Info<D, I>>,
}

#[allow(dead_code)]
impl<K, D, I> KeyToDataAndIdMap<K, D, I>
where
    K: Eq + Hash,
    I: Eq + Hash,
{
    pub(crate) fn new() -> Self {
        Self {
            inner_map: HashMap::new(),
        }
    }

    pub(crate) fn contains_key(&self, key: &K) -> bool {
        self.inner_map.contains_key(key)
    }

    pub(crate) fn contains_id_for_key(&self, key: &K, id: &I) -> bool {
        self.inner_map
            .get(key)
            .map(|info| info.ids.contains(id))
            .unwrap_or(false)
    }

    /// Insert a data into the map.
    /// Does nothing if the key is already in the map.
    pub(crate) fn insert_data(&mut self, key: K, data: D) {
        self.inner_map.entry(key).or_insert_with(|| Info {
            data,
            ids: HashSet::new(),
        });
    }

    /// Insert an id into the map.
    /// Returns true if the key is not in the map, false otherwise.
    pub(crate) fn insert_id_for_key(&mut self, key: K, data: D, id: I) -> bool {
        if let Some(info) = self.inner_map.get_mut(&key) {
            info.ids.insert(id)
        } else {
            let mut ids = HashSet::new();
            ids.insert(id);
            self.inner_map.insert(key, Info { data, ids });
            true
        }
    }

    pub(crate) fn remove_key(&mut self, key: &K) -> Option<D> {
        self.inner_map.remove(key).map(|info| info.data)
    }

    pub(crate) fn remove_id_for_key(&mut self, key: &K, id: &I) -> bool {
        if let Some(info) = self.inner_map.get_mut(key) {
            info.ids.remove(id)
        } else {
            false
        }
    }

    pub(crate) fn get_data(&self, key: &K) -> Option<&D> {
        self.inner_map.get(key).map(|info| &info.data)
    }

    pub(crate) fn get_ids_for_key(&self, key: &K) -> Option<&HashSet<I>> {
        self.inner_map.get(key).map(|info| &info.ids)
    }
}
