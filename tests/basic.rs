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
        choir.run_task(move || {
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
    let mut base = choir.idle_task(move || {});
    let n = 100;
    // Launch N tasks, each depending on the previous one
    // and each setting a value.
    // If they were running in parallel, the resulting
    // value would be undetermined. But sequentially,
    // it has to be N.
    for i in 0..n {
        let v = Arc::clone(&value);
        let next = choir.idle_task(move || {
            *v.lock().unwrap() = i + 1;
        });
        next.depend_on(&base);
        base.run();
        base = next;
    }
    base.run();
    choir.wait_idle();
    assert_eq!(*value.lock().unwrap(), n);
}
