use criterion::{criterion_group, criterion_main, Criterion};

fn many_tasks(c: &mut Criterion) {
    const TASK_COUNT: choir::SubIndex = 100_000;
    c.bench_function("individual tasks: single worker", |b| {
        let mut choir = choir::Choir::new();
        let _worker = choir.add_worker("main");
        b.iter(|| {
            let mut parent = choir.spawn("parent").init_dummy();
            for _ in 0..TASK_COUNT {
                let task = choir.spawn("").init_dummy();
                parent.depend_on(&task);
            }
            parent.run().join();
        });
    });
    let num_cores = num_cpus::get_physical();
    c.bench_function(&format!("individual tasks: {} workers", num_cores), |b| {
        let mut choir = choir::Choir::new();
        let _workers = (0..num_cores)
            .map(|i| choir.add_worker(&format!("worker-{}", i)))
            .collect::<Vec<_>>();
        b.iter(|| {
            let mut parent = choir.spawn("parent").init_dummy();
            for _ in 0..TASK_COUNT {
                let task = choir.spawn("").init_dummy();
                parent.depend_on(&task);
            }
            parent.run().join();
        });
    });
    c.bench_function("multi-task: single worker", |b| {
        let mut choir = choir::Choir::new();
        let _worker = choir.add_worker("main");
        b.iter(|| {
            choir
                .spawn("")
                .init_multi(TASK_COUNT, |_, _| {})
                .run()
                .join();
        });
    });
    c.bench_function(&format!("multi-task: {} workers", num_cores), |b| {
        let mut choir = choir::Choir::new();
        let _workers = (0..num_cores)
            .map(|i| choir.add_worker(&format!("worker-{}", i)))
            .collect::<Vec<_>>();
        b.iter(|| {
            choir
                .spawn("")
                .init_multi(TASK_COUNT, |_, _| {})
                .run()
                .join();
        });
    });
}

criterion_group!(benches, many_tasks);
criterion_main!(benches);
