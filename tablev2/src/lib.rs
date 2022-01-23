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

pub mod derive {
    pub use tablev2_macros::Queryable;
}

pub mod store;
pub use store::*;

pub struct Table<T: Queryable, S: Store<T> = VecStore<T>> {
    inner: T::Table<S>,
}

impl<T: Queryable> Table<T> {
    pub fn new() -> Self {
        Table::<T, VecStore<T>>::with_store()
    }
}

impl<T: Queryable, S: Store<T>> Table<T, S> {
    pub fn with_store() -> Self {
        Self {
            inner: T::Table::<S>::new(),
        }
    }

    pub fn query(&self) -> Query<T, S> {
        self.inner.query()
    }

    pub fn query_mut(&mut self) -> QueryMut<T, S> {
        self.inner.query_mut()
    }

    pub fn insert(&mut self, value: T) {
        self.inner.insert(value)
    }
}

pub trait Queryable: Sized {
    type Table<S: Store<Self>>: TableTrait<Self, S>;
}

pub trait TableTrait<T: Queryable, S: Store<T>> {
    fn new() -> Self;
    fn query(&self) -> Query<T, S>;
    fn query_mut(&mut self) -> QueryMut<T, S>;
    fn insert(&mut self, value: T);
    fn add_to_index(&mut self, i: usize);
    fn remove_from_index(&mut self, i: usize);

    fn store(&self) -> &S;
    fn store_mut(&mut self) -> &mut S;
}

pub struct Query<'a, T: Queryable, S: Store<T> = VecStore<T>> {
    pub table: &'a T::Table<S>,
    pub matches: Vec<QueryColumnMatches<'a>>,
    pub match_columns: HashSet<&'static str>,
}

impl<'a, T: Queryable, S: Store<T>> Query<'a, T, S> {
    pub fn get(&self) -> Matches<'a, T, S> {
        let columns = &self.matches;
        let columns_len = self.match_columns.len() as u32;

        if columns.len() == 0 {
            return Matches {
                table: self.table,
                strategy: QueryStrategy::MatchAll { current: 0 },
            };
        }

        if columns.len() < columns_len as usize {
            return Matches {
                table: self.table,
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
            table: self.table,
            strategy,
        }
    }
}

pub struct QueryMut<'a, T: Queryable, S: Store<T>> {
    pub table: &'a mut T::Table<S>,
    // Instead of keeping track of QueryColumnMatches like for a normal
    // Query, we create a match counter which is filled up immediately rather
    // than lazily during iteration.
    pub match_counter: BTreeMap<usize, u32>,
    pub match_columns: HashSet<&'static str>,
}

impl<'a, T: Queryable, S: Store<T>> QueryMut<'a, T, S> {
    pub fn new(table: &'a mut T::Table<S>) -> Self {
        Self {
            table,
            match_counter: BTreeMap::new(),
            match_columns: HashSet::new(),
        }
    }

    pub fn get<'b>(&'b self) -> Matches<'a, T, S>
    where
        'b: 'a,
    {
        Matches {
            table: self.table,
            strategy: QueryStrategy::PrefilledMatchCounter {
                match_counter: self.match_counter.clone(),
                columns_len: self.match_columns.len() as u32,
            },
        }
    }

    pub fn update(&mut self, f: impl Fn(&mut T)) {
        let matches = self.raw_matches();

        for i in matches {
            // Removing value from index
            self.table.remove_from_index(i);

            // Modifying value
            if let Some(m) = self.table.store_mut().get_mut(i) {
                f(m);
            }

            // Adding value back to the index
            self.table.add_to_index(i);
        }
    }

    pub fn remove(&'a mut self)
    where
        T: Clone,
    {
        let matches = self.raw_matches();

        for i in matches {
            self.table.store_mut().remove(i);
        }
    }

    fn raw_matches(&mut self) -> Vec<usize> {
        let mut matches = Vec::new();
        let mut query_result: Matches<_, _> = self.get();
        while let Some(i) = query_result.next_match() {
            matches.push(i);
        }
        matches
    }
}

pub struct Matches<'a, T: Queryable, S: Store<T>> {
    table: &'a T::Table<S>,
    strategy: QueryStrategy<'a>,
}

impl<'a, T: Queryable + 'a, S: Store<T>> Matches<'a, T, S> {
    pub(crate) fn next_match(&mut self) -> Option<usize> {
        self.strategy.next_match(self.table)
    }
}

impl<'a, T: Queryable + 'a, S: Store<T> + 'a> Iterator for Matches<'a, T, S> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_match().and_then(|i| self.table.store().get(i))
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
    pub fn next_match<S: Store<T>, T: Queryable, Ta: TableTrait<T, S>>(
        &mut self,
        table: &Ta,
    ) -> Option<usize> {
        use QueryStrategy::*;

        match self {
            MatchAll { ref mut current } => {
                while *current < table.store().len() {
                    *current += 1;

                    if table.store().is_free(*current - 1) {
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
    type Table<S: Store<Self>> = UserTable<S>;
}

struct UserTable<S> {
    store: S,

    name: BTreeMap<String, BTreeSet<usize>>,
    age: BTreeMap<u32, BTreeSet<usize>>,
}

impl<S> TableTrait<User, S> for UserTable<S>
where
    S: Store<User>,
{
    fn new() -> Self {
        Self {
            store: S::new(),

            name: BTreeMap::new(),
            age: BTreeMap::new(),
        }
    }

    fn query(&self) -> Query<User, S> {
        Query {
            table: self,
            matches: Vec::new(),
            match_columns: HashSet::new(),
        }
    }

    fn query_mut(&mut self) -> QueryMut<User, S> {
        QueryMut::new(self)
    }

    fn insert(&mut self, user: User) {
        let i = self.store.insert(user);
        self.add_to_index(i);
    }

    fn add_to_index(&mut self, i: usize) {
        if let Some(value) = self.store.get(i) {
            self.name.entry(value.name.clone()).or_default().insert(i);
            self.age.entry(value.age.clone()).or_default().insert(i);
        }
    }

    fn remove_from_index(&mut self, i: usize) {
        if let Some(value) = self.store.get(i) {
            self.name.get_mut(&value.name).map(|map| map.remove(&i));
            self.age.get_mut(&value.age).map(|map| map.remove(&i));
        }
    }

    fn store(&self) -> &S {
        &self.store
    }

    fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }
}

impl<'a, S: Store<User>> Query<'a, User, S> {
    pub fn name<T>(mut self, name: &T) -> Self
    where
        String: Borrow<T> + Ord,
        T: Ord + ?Sized,
    {
        self.match_columns.insert("name");
        if let Some(matches) = self.table.name.get(name) {
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
        if let Some(matches) = self.table.age.get(age) {
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
        let matches: Vec<_> = self.table.name.range(name_range).map(|(_, m)| m).collect();
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
        let matches: Vec<_> = self.table.age.range(age_range).map(|(_, m)| m).collect();
        if matches.len() > 0 {
            self.matches.push(QueryColumnMatches::Range(matches));
        }
        self
    }
}

impl<'a, S: Store<User>> QueryMut<'a, User, S> {
    pub fn name<T>(mut self, name: &T) -> Self
    where
        String: Borrow<T> + Ord,
        T: Ord + ?Sized,
    {
        self.match_columns.insert("name");
        if let Some(matches) = self.table.name.get(name) {
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
        if let Some(matches) = self.table.age.get(age) {
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
        for matches in self.table.age.range(age_range).map(|(_, m)| m) {
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

    table.query_mut().name("jon").remove();
    let query = table.query().age_range(20..31);

    let matches = query.get();

    for m in matches {
        println!("{:?}", m);
    }
}

use crate as tablev2;

#[derive(derive::Queryable, Debug, Clone)]
struct User2 {
    #[index]
    name: String,
    #[index]
    age: u32,
}

// #[test]
// fn test_user2_table() {
//     let mut table = Table::new();

//     let sara = User2 {
//         name: "sara".to_owned(),
//         age: 20,
//     };
//     let jon = User2 {
//         name: "jon".to_owned(),
//         age: 30,
//     };

//     table.insert(sara);
//     table.insert(jon);

//     table.query_mut().name("sara").update(|user| user.age = 32);

//     let query = table.query().age_range(20..31);

//     let matches = query.get();

//     for m in matches {
//         println!("{:?}", m);
//     }
// }
