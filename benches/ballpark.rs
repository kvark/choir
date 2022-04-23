use criterion::{criterion_group, criterion_main, Criterion};

fn many_tasks(c: &mut Criterion) {
    const TASK_COUNT: choir::SubIndex = 1000;
    c.bench_function("individual tasks: single worker", |b| {
        let mut choir = choir::Choir::new();
        let _worker = choir.add_worker("main");
        b.iter(|| {
            for _ in 0..TASK_COUNT {
                choir.run_task(|| {});
            }
            choir.wait_idle();
        });
    });
    let num_cores = num_cpus::get_physical();
    c.bench_function(&format!("individual tasks: {} workers", num_cores), |b| {
        let mut choir = choir::Choir::new();
        let _workers = (0..num_cores)
            .map(|i| choir.add_worker(&format!("worker-{}", i)))
            .collect::<Vec<_>>();
        b.iter(|| {
            for _ in 0..TASK_COUNT {
                choir.run_task(|| {});
            }
            choir.wait_idle();
        });
    });
    c.bench_function("multi-task: single worker", |b| {
        let mut choir = choir::Choir::new();
        let _worker = choir.add_worker("main");
        b.iter(|| {
            choir.run_multi_task(TASK_COUNT, |_| {});
            choir.wait_idle();
        });
    });
    c.bench_function(&format!("multi-task: {} workers", num_cores), |b| {
        let mut choir = choir::Choir::new();
        let _workers = (0..num_cores)
            .map(|i| choir.add_worker(&format!("worker-{}", i)))
            .collect::<Vec<_>>();
        b.iter(|| {
            choir.run_multi_task(TASK_COUNT, |_| {});
            choir.wait_idle();
        });
    });
}

criterion_group!(benches, many_tasks);
criterion_main!(benches);
