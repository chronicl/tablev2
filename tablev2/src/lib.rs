#![feature(map_first_last)]
#![feature(generic_associated_types)]

use std::collections::btree_set::Iter as BTreeSetIter;
use std::collections::HashSet;
use std::ops::RangeBounds;
use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet},
    iter::Peekable,
};

use serde::{Deserialize, Serialize};

pub mod derive {
    pub use tablev2_macros::Queryable;
}

pub mod store;
pub use store::*;

#[derive(Serialize, Deserialize)]
pub struct Table<T: Queryable, S = VecStore<T>> {
    index: T::Index,
    store: S,
}

impl<T: Queryable> Table<T> {
    pub fn new() -> Self {
        Table::<T, VecStore<T>>::with_store()
    }
}

impl<T: Queryable, S> Table<T, S> {
    pub fn with_store() -> Self
    where
        S: Store<T>,
    {
        Self {
            index: T::Index::new(),
            store: S::new(),
        }
    }

    pub fn query<'a>(&'a self) -> T::Query<'a, S>
    where
        S: 'static,
    {
        self.index.query(&self.store)
    }

    pub fn query_mut<'a>(&'a mut self) -> T::QueryMut<'a, S>
    where
        S: 'static,
    {
        self.index.query_mut(&mut self.store)
    }

    pub fn insert(&mut self, value: T)
    where
        S: Store<T>,
    {
        let i = self.store.insert(value);
        self.index.add_to_index(&mut self.store, i);
    }
}

pub trait Queryable: Sized {
    type Index: Index<Self>;
    type Query<'a, S: 'static>: Query<Self, S>;
    type QueryMut<'a, S: 'static>: QueryMut<Self, S>;
}

pub trait Index<T: Queryable> {
    fn new() -> Self;
    fn query<'a, S: 'static>(&'a self, store: &'a S) -> T::Query<'a, S>;
    fn query_mut<'a, S: 'static>(&'a mut self, store: &'a mut S) -> T::QueryMut<'a, S>;
    fn add_to_index<S: Store<T>>(&mut self, store: &mut S, i: usize);
    fn remove_from_index<S: Store<T>>(&mut self, store: &mut S, i: usize);
}

pub trait Query<T: Queryable, S = VecStore<T>> {
    fn get<'a>(&'a self) -> Matches<'a, T, S>;
}

pub trait QueryMut<T: Queryable, S = VecStore<T>> {
    fn get<'a>(&'a self) -> Matches<'a, T, S>;
    fn update(&mut self, f: impl Fn(&mut T))
    where
        S: Store<T>;
    fn remove(&mut self)
    where
        S: Store<T>;
}

pub struct Matches<'a, T: Queryable, S> {
    pub index: &'a T::Index,
    pub store: &'a S,
    pub strategy: QueryStrategy<'a>,
}

impl<'a, T: Queryable + 'a, S: Store<T>> Matches<'a, T, S> {
    pub fn next_match(&mut self) -> Option<usize> {
        self.strategy.next_match(self.store)
    }
}

impl<'a, T: Queryable + 'a, S: Store<T> + 'a> Iterator for Matches<'a, T, S> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_match().and_then(|i| self.store.get(i))
    }
}

#[derive(Debug, Clone)]
pub enum QueryColumnMatches<'a> {
    Selected(&'a BTreeSet<usize>),
    Range(Vec<&'a BTreeSet<usize>>),
}

#[derive(Clone)]
pub enum QueryStrategy<'a> {
    MatchAll {
        current: usize,
    },
    MatchCounter {
        match_counter: BTreeMap<usize, u32>,
        matches: Vec<Peekable<BTreeSetIter<'a, usize>>>,
        columns_len: u32,
    },
    PrefilledMatchCounter {
        match_counter: BTreeMap<usize, u32>,
        columns_len: u32,
    },
    NoMatches,
}

impl<'a> QueryStrategy<'a> {
    pub fn next_match<S: Store<T>, T: Queryable>(&mut self, store: &S) -> Option<usize> {
        use QueryStrategy::*;

        match self {
            MatchAll { ref mut current } => {
                while *current < store.len() {
                    *current += 1;

                    if store.is_free(*current - 1) {
                        continue;
                    }

                    return Some(*current - 1);
                }
                return None;
            }
            MatchCounter {
                match_counter,
                matches,
                columns_len,
            } => {
                loop {
                    let mut all_columns_consumed = true;

                    let mut lowest_row_match =
                        **matches.iter_mut().find_map(|column| column.peek())?;

                    for column in matches.iter_mut() {
                        // We go through all matches of this column which have a lower
                        // index than lowest_row_match. This is more cache efficient
                        // than going one match at a time per column (column is a BTreeSet).
                        while let Some(&match_) = column.next() {
                            all_columns_consumed = false;

                            let match_counter = match_counter.entry(match_).or_default();
                            *match_counter += 1;
                            if *match_counter == *columns_len {
                                return Some(match_);
                            };

                            if lowest_row_match < match_ {
                                lowest_row_match = match_;
                                break;
                            }
                        }
                    }

                    if all_columns_consumed {
                        return None;
                    }
                }
            }
            PrefilledMatchCounter {
                ref mut match_counter,
                columns_len,
            } => {
                while let Some((i, counter)) = match_counter.pop_first() {
                    if counter == *columns_len {
                        return Some(i);
                    }
                }
                return None;
            }
            NoMatches => return None,
        }
    }
}

/// Example implementation. This is what will be generated if you derive
/// Queryable for `User` as follows
///
/// #[derive(Queryable)]
/// struct User {
///     #[index]
///     name: String,
///     #[index]
///     age: u32,
/// }
#[derive(Debug, Clone)]
struct User {
    name: String,
    age: u32,
}

impl Queryable for User {
    type Index = UserIndex;
    type Query<'a, S: 'static> = UserQuery<'a, S>;
    type QueryMut<'a, S: 'static> = UserQueryMut<'a, S>;
}

struct UserIndex {
    name: BTreeMap<String, BTreeSet<usize>>,
    age: BTreeMap<u32, BTreeSet<usize>>,
}

struct UserQuery<'a, S> {
    pub index: &'a <User as Queryable>::Index,
    pub store: &'a S,
    pub matches: Vec<QueryColumnMatches<'a>>,
    pub match_columns: HashSet<&'static str>,
}

struct UserQueryMut<'a, S> {
    pub index: &'a mut <User as Queryable>::Index,
    pub store: &'a mut S,
    // Instead of keeping track of QueryColumnMatches like for a normal
    // Query, we create a match counter which is filled up immediately rather
    // than lazily during iteration.
    pub match_counter: BTreeMap<usize, u32>,
    pub match_columns: HashSet<&'static str>,
}

impl Index<User> for UserIndex {
    fn new() -> Self {
        Self {
            name: BTreeMap::new(),
            age: BTreeMap::new(),
        }
    }

    fn query<'a, S>(&'a self, store: &'a S) -> <User as Queryable>::Query<'a, S>
    where
        S: 'static,
    {
        UserQuery {
            index: self,
            store,
            matches: Vec::new(),
            match_columns: HashSet::new(),
        }
    }

    fn query_mut<'a, S>(&'a mut self, store: &'a mut S) -> <User as Queryable>::QueryMut<'a, S>
    where
        S: 'static,
    {
        UserQueryMut {
            index: self,
            store,
            match_counter: BTreeMap::new(),
            match_columns: HashSet::new(),
        }
    }

    fn add_to_index<S: Store<User>>(&mut self, store: &mut S, i: usize) {
        if let Some(value) = store.get(i) {
            self.name.entry(value.name.clone()).or_default().insert(i);
            self.age.entry(value.age.clone()).or_default().insert(i);
        }
    }

    fn remove_from_index<S: Store<User>>(&mut self, store: &mut S, i: usize) {
        if let Some(value) = store.get(i) {
            self.name.get_mut(&value.name).map(|map| map.remove(&i));
            self.age.get_mut(&value.age).map(|map| map.remove(&i));
        }
    }
}

impl<'a, S> Query<User, S> for UserQuery<'a, S> {
    fn get<'b>(&'b self) -> Matches<'b, User, S> {
        let columns = &self.matches;
        let columns_len = self.match_columns.len() as u32;

        if columns.len() == 0 {
            return Matches {
                index: self.index,
                store: self.store,
                strategy: QueryStrategy::MatchAll { current: 0 },
            };
        }

        if columns.len() < columns_len as usize {
            return Matches {
                index: self.index,
                store: self.store,
                strategy: QueryStrategy::NoMatches,
            };
        }

        let mut matches = Vec::new();

        for column in columns.iter() {
            match column {
                QueryColumnMatches::Selected(column) => matches.push(column.iter().peekable()),
                QueryColumnMatches::Range(column) => {
                    for c in column {
                        matches.push(c.iter().peekable())
                    }
                }
            }
        }

        let strategy = QueryStrategy::MatchCounter {
            match_counter: BTreeMap::new(),
            matches,
            columns_len,
        };

        Matches {
            index: self.index,
            store: self.store,
            strategy,
        }
    }
}

impl<'a, S> QueryMut<User, S> for UserQueryMut<'a, S> {
    fn get<'b>(&'b self) -> Matches<'b, User, S> {
        Matches {
            index: self.index,
            store: self.store,
            strategy: QueryStrategy::PrefilledMatchCounter {
                match_counter: self.match_counter.clone(),
                columns_len: self.match_columns.len() as u32,
            },
        }
    }

    fn update(&mut self, f: impl Fn(&mut User))
    where
        S: Store<User>,
    {
        let mut matches = Vec::new();
        let mut query_result: Matches<_, _> = self.get();
        while let Some(i) = query_result.next_match() {
            matches.push(i);
        }

        for i in matches {
            // Removing value from index
            self.index.remove_from_index(self.store, i);

            // Modifying value
            if let Some(m) = self.store.get_mut(i) {
                f(m);
            }

            // Adding value back to the index
            self.index.add_to_index(self.store, i);
        }
    }

    fn remove(&mut self)
    where
        S: Store<User>,
    {
        let mut matches = Vec::new();
        let mut query_result: Matches<_, _> = self.get();
        while let Some(i) = query_result.next_match() {
            matches.push(i);
        }

        for i in matches {
            self.index.remove_from_index(self.store, i);
            self.store.remove(i);
        }
    }
}

impl<'a, S> UserQuery<'a, S> {
    pub fn name<T>(mut self, name: &T) -> Self
    where
        String: Borrow<T> + Ord,
        T: Ord + ?Sized,
    {
        self.match_columns.insert("name");
        if let Some(matches) = self.index.name.get(name) {
            self.matches.push(QueryColumnMatches::Selected(matches));
        }
        self
    }

    pub fn age<T>(mut self, age: &T) -> Self
    where
        u32: Borrow<T> + Ord,
        T: Ord + ?Sized,
    {
        self.match_columns.insert("age");
        if let Some(matches) = self.index.age.get(age) {
            self.matches.push(QueryColumnMatches::Selected(matches));
        }
        self
    }

    pub fn name_range<T, R>(mut self, name_range: R) -> Self
    where
        String: Borrow<T> + Ord,
        T: Ord + ?Sized,
        R: RangeBounds<T>,
    {
        self.match_columns.insert("name");
        let matches: Vec<_> = self.index.name.range(name_range).map(|(_, m)| m).collect();
        if matches.len() > 0 {
            self.matches.push(QueryColumnMatches::Range(matches));
        }
        self
    }

    pub fn age_range<T, R>(mut self, age_range: R) -> Self
    where
        u32: Borrow<T> + Ord,
        T: Ord + ?Sized,
        R: RangeBounds<T>,
    {
        self.match_columns.insert("age");
        let matches: Vec<_> = self.index.age.range(age_range).map(|(_, m)| m).collect();
        if matches.len() > 0 {
            self.matches.push(QueryColumnMatches::Range(matches));
        }
        self
    }
}

impl<'a, S> UserQueryMut<'a, S> {
    pub fn name<T>(mut self, name: &T) -> Self
    where
        String: Borrow<T> + Ord,
        T: Ord + ?Sized,
    {
        self.match_columns.insert("name");
        if let Some(matches) = self.index.name.get(name) {
            for m in matches.iter() {
                let counter = self.match_counter.entry(*m).or_default();
                *counter += 1;
            }
        }
        self
    }

    pub fn age<T>(mut self, age: &T) -> Self
    where
        u32: Borrow<T> + Ord,
        T: Ord + ?Sized,
    {
        self.match_columns.insert("age");
        if let Some(matches) = self.index.age.get(age) {
            for m in matches.iter() {
                let counter = self.match_counter.entry(*m).or_default();
                *counter += 1;
            }
        }
        self
    }

    pub fn age_range<T, R>(mut self, age_range: R) -> Self
    where
        u32: Borrow<T> + Ord,
        T: Ord + ?Sized,
        R: RangeBounds<T>,
    {
        self.match_columns.insert("age");
        for matches in self.index.age.range(age_range).map(|(_, m)| m) {
            for m in matches.iter() {
                let counter = self.match_counter.entry(*m).or_default();
                *counter += 1;
            }
        }
        self
    }
}

#[test]
fn test_user_table() {
    let mut table = Table::new();

    let sara = User {
        name: "sara".to_owned(),
        age: 20,
    };
    let jon = User {
        name: "jon".to_owned(),
        age: 30,
    };

    table.insert(sara);
    table.insert(jon);

    // table.query_mut().name("jon").remove();
    let query = table.query().age_range(20..31);

    let matches = query.get();

    for m in matches {
        println!("{:?}", m);
    }
}

mod tests {
    use crate as tablev2;
    use tablev2::{Query, QueryMut, Table};

    #[derive(tablev2::derive::Queryable, Debug, Clone)]
    struct User2 {
        #[index]
        name: String,
        #[index]
        age: u32,
    }

    #[test]
    fn test_user2_table() {
        let mut table = Table::new();

        let sara = User2 {
            name: "sara".to_owned(),
            age: 20,
        };
        let jon = User2 {
            name: "jon".to_owned(),
            age: 30,
        };

        table.insert(sara);
        table.insert(jon);

        table.query_mut().name("sara").update(|user| user.age = 32);

        let query = table.query().age_range(20..31);

        let matches = query.get();

        for m in matches {
            println!("{:?}", m);
        }
    }
}
