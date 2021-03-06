use std::{collections::BTreeSet, marker::PhantomData};

pub trait Store<T>: Sized {
    fn new() -> Self;
    fn get(&self, i: usize) -> Option<&T>;
    fn get_mut(&mut self, i: usize) -> Option<&mut T>;
    fn insert(&mut self, value: T) -> usize;
    fn remove(&mut self, i: usize) -> Option<T>
    where
        T: Clone;
    /// If `is_free` returns false and `get` returns None then
    /// we've reached the end of the store.
    fn is_free(&self, i: usize) -> bool;
    fn len(&self) -> usize;
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VecStore<T> {
    rows: Vec<T>,
    free_rows: BTreeSet<usize>,
}

impl<T> Store<T> for VecStore<T> {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            free_rows: BTreeSet::new(),
        }
    }

    fn get(&self, i: usize) -> Option<&T> {
        if !self.free_rows.contains(&i) {
            self.rows.get(i)
        } else {
            return None;
        }
    }

    fn get_mut(&mut self, i: usize) -> Option<&mut T> {
        if !self.free_rows.contains(&i) {
            self.rows.get_mut(i)
        } else {
            return None;
        }
    }

    fn insert(&mut self, value: T) -> usize {
        if let Some(i) = self.free_rows.pop_first() {
            self.rows[i] = value;
            i
        } else {
            let i = self.rows.len();
            self.rows.push(value);
            i
        }
    }

    fn remove(&mut self, i: usize) -> Option<T>
    where
        T: Clone,
    {
        self.free_rows.insert(i);
        self.rows.get(i).map(|r| r.to_owned())
    }

    fn is_free(&self, i: usize) -> bool {
        self.free_rows.contains(&i)
    }

    fn len(&self) -> usize {
        self.rows.len()
    }
}
