use std::marker::PhantomData;
use storage::{Storage, TableId};

mod storage;


const COUNTER_TABLE_ID: TableId<(), u64> = TableId { id: 0, key: PhantomData, value: PhantomData };

fn main() -> anyhow::Result<()> {
    let mut store = Storage::new();
    store.put_table_entry(&COUNTER_TABLE_ID, (), 3);

    let counter = store.borrow_table_entry_mut(&COUNTER_TABLE_ID, &())?;
    if *counter > 0 {
        *counter -= 1;
    }

    println!("counter = {}", *counter);

    Ok(())
}
