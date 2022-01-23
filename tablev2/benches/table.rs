#![feature(generic_associated_types)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::hash::Hash;
use tablev2::{Query, Queryable, Table, VecStore};

fn table_get(query: &impl Query<User, VecStore<User>>) {
    query.get().count();
}

#[derive(tablev2::derive::Queryable, Clone)]
struct User {
    #[index]
    name: String,
    #[index]
    age: u32,
}

impl User {
    fn new(name: &str, age: u32) -> Self {
        Self {
            name: name.to_owned(),
            age,
        }
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut table = Table::new();
    let jon20 = User::new("jon", 20);
    let jon30 = User::new("jon", 30);
    let sara20 = User::new("sara", 20);
    for _ in 0..10000 {
        table.insert(jon20.clone());
        table.insert(jon30.clone());
        table.insert(jon30.clone());
        table.insert(sara20.clone());
    }

    let jon20_query = table.query().name("jon").age(&20);
    let jon_query = table.query().name("jon");
    let sara20_query = table.query().name("sara").age(&20);

    // (first query matches, second query matches) -> (matching both)
    c.bench_function("table (3/4, 2/4) -> (1/4)", |b| {
        b.iter(|| table_get(black_box(&jon20_query)))
    });

    c.bench_function("table (3/4, 1) -> (3/4)", |b| {
        b.iter(|| table_get(black_box(&jon_query)))
    });

    c.bench_function("table (1/4, 2/4) -> (0)", |b| {
        b.iter(|| table_get(black_box(&sara20_query)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
