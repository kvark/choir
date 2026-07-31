use choir::arc::Linearc;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;

const FAN_INS: &[usize] = &[1, 2, 8, 64, 1_024];

fn linearc_sequential(fan_in: usize) -> usize {
    let root = Linearc::new(black_box(42usize));
    let mut handles = Vec::with_capacity(fan_in);
    for _ in 0..fan_in {
        handles.push(Linearc::clone(&root));
    }
    drop(root);

    let mut winners = 0;
    for handle in handles {
        if Linearc::into_inner(handle).is_some() {
            winners += 1;
        }
    }
    winners
}

fn arc_sequential(fan_in: usize) -> usize {
    let root = Arc::new(black_box(42usize));
    let mut handles = Vec::with_capacity(fan_in);
    for _ in 0..fan_in {
        handles.push(Arc::clone(&root));
    }
    drop(root);

    let mut winners = 0;
    for handle in handles {
        if Arc::into_inner(handle).is_some() {
            winners += 1;
        }
    }
    winners
}

fn linearc_contended(fan_in: usize) -> usize {
    let root = Linearc::new(black_box(42usize));
    let handles = (0..fan_in)
        .map(|_| Linearc::clone(&root))
        .collect::<Vec<_>>();
    drop(root);

    std::thread::scope(|scope| {
        handles
            .into_iter()
            .map(|handle| scope.spawn(move || usize::from(Linearc::into_inner(handle).is_some())))
            .collect::<Vec<_>>()
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .sum()
    })
}

fn arc_contended(fan_in: usize) -> usize {
    let root = Arc::new(black_box(42usize));
    let handles = (0..fan_in)
        .map(|_| Arc::clone(&root))
        .collect::<Vec<_>>();
    drop(root);

    std::thread::scope(|scope| {
        handles
            .into_iter()
            .map(|handle| scope.spawn(move || usize::from(Arc::into_inner(handle).is_some())))
            .collect::<Vec<_>>()
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .sum()
    })
}

fn refcount_gate(c: &mut Criterion) {
    let mut sequential = c.benchmark_group("refcount_gate/sequential");
    for &fan_in in FAN_INS {
        sequential.bench_with_input(BenchmarkId::new("linearc", fan_in), &fan_in, |b, &n| {
            b.iter(|| assert_eq!(black_box(linearc_sequential(n)), 1));
        });
        sequential.bench_with_input(BenchmarkId::new("arc_into_inner", fan_in), &fan_in, |b, &n| {
            b.iter(|| assert_eq!(black_box(arc_sequential(n)), 1));
        });
    }
    sequential.finish();

    let mut contended = c.benchmark_group("refcount_gate/contended");
    for &fan_in in &[2, 4, 8, 16] {
        contended.bench_with_input(BenchmarkId::new("linearc", fan_in), &fan_in, |b, &n| {
            b.iter(|| assert_eq!(black_box(linearc_contended(n)), 1));
        });
        contended.bench_with_input(BenchmarkId::new("arc_into_inner", fan_in), &fan_in, |b, &n| {
            b.iter(|| assert_eq!(black_box(arc_contended(n)), 1));
        });
    }
    contended.finish();
}

criterion_group!(benches, refcount_gate);
criterion_main!(benches);
