use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

#[test]
fn parallel() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _worker1 = choir.add_worker("P1");
    let _worker2 = choir.add_worker("P2");

    let value = Arc::new(AtomicUsize::new(0));
    let n = 100;
    // Launch N independent tasks, each bumping
    // the value. Expect all of them to work.
    let mut last = choir.spawn("last").init_dummy();
    for _ in 0..n {
        let v = Arc::clone(&value);
        let child = choir
            .spawn("")
            .init(move |_| {
                v.fetch_add(1, Ordering::AcqRel);
            })
            .run();
        last.depend_on(&child);
    }

    last.run().join();
    assert_eq!(value.load(Ordering::Acquire), n);
}

#[test]
fn sequential() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _worker = choir.add_worker("S");

    let value = Arc::new(Mutex::new(0));
    let mut base = choir.spawn("base").init_dummy();
    let n = 100;
    // Launch N tasks, each depending on the previous one
    // and each setting a value.
    // If they were running in parallel, the resulting
    // value would be undetermined. But sequentially,
    // it has to be N.
    for i in 0..n {
        let v = Arc::clone(&value);
        let mut next = choir.spawn("").init(move |_| {
            *v.lock().unwrap() = i + 1;
        });
        next.depend_on(&base);
        base = next;
    }
    base.run().join();
    assert_eq!(*value.lock().unwrap(), n);
}

#[test]
fn zero_count() {
    let choir = choir::Choir::new();
    let _worker1 = choir.add_worker("A");
    choir.spawn("").init_multi(0, |_, _| {}).run().join();
}

#[test]
fn multi_sum() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _worker1 = choir.add_worker("A");
    let _worker2 = choir.add_worker("B");

    let value = Arc::new(AtomicUsize::new(0));
    let value_other = Arc::clone(&value);
    let n = 100;
    choir
        .spawn("")
        .init_multi(n, move |_, i| {
            value_other.fetch_add(i as usize, Ordering::SeqCst);
        })
        .run()
        .join();
    assert_eq!(value.load(Ordering::Acquire) as u32, (n - 1) * n / 2);
}

#[test]
fn iter_xor() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _worker1 = choir.add_worker("A");
    let _worker2 = choir.add_worker("B");

    let value = Arc::new(AtomicUsize::new(0));
    let value_other = Arc::clone(&value);
    let n = 50;

    choir
        .spawn("")
        .init_iter(0..n, move |item| {
            value_other.fetch_xor(item, Ordering::SeqCst);
        })
        .run()
        .join();
    assert_eq!(value.load(Ordering::Acquire), 1);
}

#[test]
fn proxy() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _worker1 = choir.add_worker("A");
    let _worker2 = choir.add_worker("B");

    let value = Arc::new(AtomicUsize::new(0));
    let value_other = Arc::clone(&value);
    let n = 50;
    choir
        .spawn("parent")
        .init_multi(n, move |ec, i| {
            println!("base[{}]", i);
            let value_other2 = Arc::clone(&value_other);
            value_other.fetch_or(1 << i, Ordering::SeqCst);
            ec.fork("proxy").init(move |_| {
                println!("proxy[{}]", i);
                value_other2.fetch_xor(1 << i, Ordering::SeqCst);
            });
        })
        .run()
        .join();

    assert_eq!(value.load(Ordering::Acquire), 0);
}

#[test]
fn fork_in_flight() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _worker1 = choir.add_worker("A");
    let value = Arc::new(AtomicUsize::new(0));
    let value1 = Arc::clone(&value);
    // This task deliberately waits, so that we know for sure
    // if it's being waited on.
    let t1 = choir
        .spawn("child")
        .init(move |_| {
            thread::sleep(Duration::from_millis(10));
            value1.fetch_add(1, Ordering::AcqRel);
        })
        .run();
    let value2 = Arc::clone(&value);
    // This task decides to add `t1` as a fork, which is already in flight.
    choir
        .spawn("parent")
        .init(move |ec| {
            value2.fetch_add(1, Ordering::AcqRel);
            ec.add_fork(&t1);
        })
        .run()
        .join();
    assert_eq!(value.load(Ordering::Acquire), 2);
}

#[test]
fn unhelpful() {
    let choir = choir::Choir::new();
    let mut done = false;
    choir
        .spawn("task")
        .init(|_| {
            done = true;
        })
        .run_attached();
    assert!(done);
}

#[test]
fn multi_thread_join() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _w = choir.add_worker("main");
    let running = choir
        .spawn("task")
        .init(|_| {
            thread::sleep(Duration::from_millis(100));
        })
        .run();
    let r1 = running.clone();
    let t1 = thread::spawn(move || r1.join());
    let r2 = running.clone();
    let t2 = thread::spawn(move || r2.join());
    t1.join().unwrap();
    t2.join().unwrap();
}

#[test]
#[should_panic]
fn task_panic() {
    let choir = choir::Choir::new();
    let _w = choir.add_worker("main");
    choir.spawn("task").init(|_| panic!("Oops!")).run().join();
}
