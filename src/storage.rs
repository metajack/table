#![allow(dead_code)]

use anyhow::Result;
use serde::{Deserialize, Deserializer, de::DeserializeOwned, Serialize};
use std::{
    any::{TypeId},
    collections::HashMap,
    marker::PhantomData,
};

pub trait TableValue: erased_serde::Serialize + Send + Sync + 'static {
    fn type_id(&self, _: private::Internal) -> TypeId {
        TypeId::of::<Self>()
    }

    fn deserialize<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        Self: Sized,
        D: Deserializer<'de>;
}

impl dyn TableValue {
    pub fn is<T: TableValue>(&self) -> bool {
        let t = TypeId::of::<T>();
        let boxed = self.type_id(private::Internal);
        t == boxed
    }

    pub fn downcast_ref<T: TableValue>(&self) -> Option<&T> {
        if self.is::<T>() {
            unsafe { Some(&*(self as *const dyn TableValue as *const T)) }
        } else {
            None
        }
    }

    pub fn downcast_mut<T: TableValue>(&mut self) -> Option<&mut T> {
        if self.is::<T>() {
            unsafe { Some(&mut *(self as *mut dyn TableValue as *mut T)) }
        } else {
            None
        }
    }
}

impl<T> TableValue for T
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn deserialize<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        Self: Sized,
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer)
    }
}

pub struct TableId<K: Serialize, V: TableValue> {
    pub id: u64,
    pub key: PhantomData<K>,
    pub value: PhantomData<V>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TableEntry {
    id: u64,
    key: Vec<u8>,
}

impl TableEntry {
    fn from_key<K: Serialize>(table_id: u64, key: &K) -> TableEntry {
        TableEntry {
            id: table_id,
            key: serde_json::to_vec(key).unwrap(),
        }
    }
}

pub struct Storage {
    entries: HashMap<TableEntry, Box<dyn TableValue>>,
    database: HashMap<TableEntry, Vec<u8>>,
}

impl Storage {
    pub fn new() -> Storage {
        Storage {
            entries: HashMap::new(),
            database: HashMap::new(),
        }
    }

    fn ensure_cached_table_entry<
        V: TableValue,
    >(
        &mut self,
        table_entry: &TableEntry,
    ) -> Result<()> {
        if self.entries.contains_key(table_entry) {
            return Ok(());
        }
        if !self.database.contains_key(table_entry) {
            return Ok(());
        }
        let bytes = self.database.get(table_entry).unwrap();
        let mut de = serde_json::Deserializer::from_slice(bytes);
        let value = V::deserialize(&mut de)?;
        self.entries.insert(table_entry.clone(), Box::new(value));
        Ok(())
    }

    pub fn contains_table_entry<
        K: Serialize,
        V: TableValue,
    >(
        &mut self,
        table_id: &TableId<K, V>,
        key: &K,
    ) -> Result<bool> {
        let table_entry = TableEntry::from_key(table_id.id, &key);
        if self.entries.contains_key(&table_entry) {
            return Ok(true);
        }
        if self.database.contains_key(&table_entry) {
            return Ok(true);
        }
        Ok(false)
    }

    pub fn put_table_entry<
        K: Serialize,
        V: TableValue,
    >(&mut self, table_id: &TableId<K, V>, key: K, value: V) {
        let table_entry = TableEntry::from_key(table_id.id, &key);
        let mut writer = Vec::new();
        let mut json_serializer = serde_json::Serializer::new(&mut writer);
        let mut erased_json_serializer = <dyn erased_serde::Serializer>::erase(&mut json_serializer);
        value.erased_serialize(&mut erased_json_serializer).unwrap();
        self.database.insert(table_entry.clone(), writer);
        self.entries.insert(table_entry, Box::new(value));
    }

    pub fn borrow_table_entry<
        K: Serialize,
        V: TableValue,
    >(
        &mut self,
        table_id: &TableId<K, V>,
        key: &K,
    ) -> Result<&V> {
        let table_entry = TableEntry::from_key(table_id.id, &key);
        self.ensure_cached_table_entry::<V>(&table_entry)?;
        let entry = self.entries.get(&table_entry).unwrap();
        Ok(entry.downcast_ref::<V>().unwrap())
    }

    pub fn borrow_table_entry_mut<
        K: Serialize,
        V: TableValue,
    >(
        &mut self,
        table_id: &TableId<K, V>,
        key: &K,
    ) -> Result<&mut V> {
        let table_entry = TableEntry::from_key(table_id.id, &key);
        self.ensure_cached_table_entry::<V>(&table_entry)?;
        let entry = self.entries.get_mut(&table_entry).unwrap();
        Ok(entry.downcast_mut::<V>().unwrap())
    }
}

mod private {
    #[derive(Debug)]
    pub struct Internal;
}
