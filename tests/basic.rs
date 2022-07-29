use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[test]
fn parallel() {
    let _ = env_logger::try_init();
    let mut choir = choir::Choir::new();
    let _worker1 = choir.add_worker("P1");
    let _worker2 = choir.add_worker("P2");

    let value = Arc::new(AtomicUsize::new(0));
    let n = 100;
    // Launch N independent tasks, each bumping
    // the value. Expect all of them to work.
    for _ in 0..n {
        let v = Arc::clone(&value);
        choir.spawn("").init(move |_| {
            v.fetch_add(1, Ordering::AcqRel);
        });
    }

    choir.wait_idle();
    assert_eq!(value.load(Ordering::Acquire), n);
}

#[test]
fn sequential() {
    let _ = env_logger::try_init();
    let mut choir = choir::Choir::new();
    let _worker = choir.add_worker("S");

    let value = Arc::new(Mutex::new(0));
    let mut base = choir.spawn("").init(move |_| {});
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
    base.run();
    choir.wait_idle();
    assert_eq!(*value.lock().unwrap(), n);
}

#[test]
fn zero_count() {
    let mut choir = choir::Choir::new();
    let _worker1 = choir.add_worker("A");
    choir.spawn("").init_multi(0, |_, _| {});
    choir.wait_idle();
}

#[test]
fn multi_sum() {
    let _ = env_logger::try_init();
    let mut choir = choir::Choir::new();
    let _worker1 = choir.add_worker("A");
    let _worker2 = choir.add_worker("B");

    let value = Arc::new(AtomicUsize::new(0));
    let value_other = Arc::clone(&value);
    let n = 100;
    choir.spawn("").init_multi(n, move |_, i| {
        value_other.fetch_add(i as usize, Ordering::SeqCst);
    });
    choir.wait_idle();
    assert_eq!(value.load(Ordering::Acquire) as u32, (n - 1) * n / 2);
}

#[test]
fn iter_xor() {
    let _ = env_logger::try_init();
    let mut choir = choir::Choir::new();
    let _worker1 = choir.add_worker("A");
    let _worker2 = choir.add_worker("B");

    let value = Arc::new(AtomicUsize::new(0));
    let value_other = Arc::clone(&value);
    let n = 50;

    choir.spawn("").init_iter(0..n, move |item| {
        value_other.fetch_xor(item, Ordering::SeqCst);
    });
    choir.wait_idle();
    assert_eq!(value.load(Ordering::Acquire), 1);
}

#[test]
fn proxy() {
    let _ = env_logger::try_init();
    let mut choir = choir::Choir::new();
    let _worker1 = choir.add_worker("A");
    let _worker2 = choir.add_worker("B");
    let choir_arc = Arc::new(choir);

    let value = Arc::new(AtomicUsize::new(0));
    let choir_other = Arc::clone(&choir_arc);
    let value_other = Arc::clone(&value);
    let n = 50;
    let compute = choir_arc.spawn("parent").init_multi(n, move |notifier, i| {
        let value_other2 = Arc::clone(&value_other);
        value_other.fetch_or(1 << i, Ordering::SeqCst);
        choir_other.spawn_proxy("proxy", notifier).init(move |_| {
            value_other2.fetch_xor(1 << i, Ordering::SeqCst);
        });
    });
    choir_arc
        .spawn("test")
        .init(move |_| {
            assert_eq!(value.load(Ordering::Acquire), 0);
        })
        .depend_on(&compute);
    choir_arc.wait_idle();
}
