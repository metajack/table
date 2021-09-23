#![allow(dead_code)]

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::PhantomData,
};

pub trait TableValue: erased_serde::Serialize + Send + Sync + 'static {
    fn type_id(&self, _: private::Internal) -> TypeId {
        TypeId::of::<Self>()
    }
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
    T: Serialize + Any + Send + Sync + 'static
{}

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
}

impl Storage {
    pub fn new() -> Storage {
        Storage {
            entries: HashMap::new(),
        }
    }

    pub fn put_table_entry<
        K: Serialize,
        V: TableValue,
    >(&mut self, table_id: &TableId<K, V>, key: K, value: V) {
        let table_entry = TableEntry::from_key(table_id.id, &key);
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
        let entry = self.entries.get_mut(&table_entry).unwrap();
        Ok(entry.downcast_mut::<V>().unwrap())
    }
}

mod private {
    #[derive(Debug)]
    pub struct Internal;
}
