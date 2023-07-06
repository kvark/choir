use criterion::{criterion_group, criterion_main, Criterion};

fn work(n: choir::SubIndex) {
    let mut vec = vec![1, 1];
    for i in 2..n as usize {
        vec.push(vec[i - 1] + vec[i - 2]);
    }
}

fn many_tasks(c: &mut Criterion) {
    const TASK_COUNT: choir::SubIndex = 1_000;
    //let _ = profiling::tracy_client::Client::start();
    let choir = choir::Choir::new();
    if true {
        let _worker = choir.add_worker("main");
        c.bench_function("individual tasks: single worker", |b| {
            b.iter(|| {
                let mut parent = choir.spawn("parent").init_dummy();
                for i in 0..TASK_COUNT {
                    let task = choir.spawn("").init(move |_| work(i));
                    parent.depend_on(&task);
                }
                parent.run().join();
            });
        });
        c.bench_function("multi-task: single worker", |b| {
            b.iter(|| {
                choir
                    .spawn("")
                    .init_multi(TASK_COUNT, move |_, i| work(i))
                    .run()
                    .join();
            });
        });
    }

    if true {
        let num_cores = num_cpus::get_physical();
        let _workers = (0..num_cores)
            .map(|i| choir.add_worker(&format!("worker-{}", i)))
            .collect::<Vec<_>>();
        c.bench_function(&format!("individual tasks: {} workers", num_cores), |b| {
            b.iter(|| {
                let mut parent = choir.spawn("parent").init_dummy();
                for i in 0..TASK_COUNT {
                    let task = choir.spawn("").init(move |_| work(i));
                    parent.depend_on(&task);
                }
                parent.run().join();
            });
        });
        c.bench_function(&format!("multi-task: {} workers", num_cores), |b| {
            b.iter(|| {
                choir
                    .spawn("")
                    .init_multi(TASK_COUNT, move |_, i| work(i))
                    .run()
                    .join();
            });
        });
    }
}

criterion_group!(benches, many_tasks);
criterion_main!(benches);
