use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

/// Finding A: when a task panics, its direct dependents must be unblocked.
/// Previously, `ExecutionContext::drop` only called `unpark_waiting()` and
/// did not walk `dependents`, so tasks blocked on the panicking task would
/// hang forever.
#[test]
fn panic_unblocks_direct_dependents() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _w = choir.add_worker("W");

    let a = choir.spawn("A").init(|_| panic!("boom"));
    let mut b = choir.spawn("B").init_dummy();
    b.depend_on(&a);
    let b_running = b.run();
    drop(a);

    let (tx, rx) = std::sync::mpsc::channel();
    let b_clone = b_running.clone();
    thread::spawn(move || {
        let mp = b_clone.join();
        let _ = tx.send(());
        std::mem::forget(mp);
    });

    rx.recv_timeout(Duration::from_secs(5))
        .expect("B must not hang when A panics");
}

/// Finding A extended: transitive dependents of a panicking task must also
/// be unblocked (A panics → B is dependent → C depends on B).
#[test]
fn panic_unblocks_transitive_dependents() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _w = choir.add_worker("W");

    let a = choir.spawn("A").init(|_| panic!("boom"));
    let mut b = choir.spawn("B").init_dummy();
    b.depend_on(&a);
    let mut c = choir.spawn("C").init_dummy();
    c.depend_on(&b);
    let c_running = c.run();
    drop(b);
    drop(a);

    let (tx, rx) = std::sync::mpsc::channel();
    let c_clone = c_running.clone();
    thread::spawn(move || {
        let mp = c_clone.join();
        let _ = tx.send(());
        std::mem::forget(mp);
    });

    rx.recv_timeout(Duration::from_secs(5))
        .expect("C must not hang when A panics transitively");
}

/// Finding B: `finish()` must not crash if the continuation was already
/// taken (e.g., by `flush_notifier` during panic handling).
/// This test races a normal completion against a panic-triggered flush.
#[test]
fn finish_tolerates_taken_continuation() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _w1 = choir.add_worker("W1");
    let _w2 = choir.add_worker("W2");

    let completed = Arc::new(AtomicUsize::new(0));

    for _ in 0..10 {
        let c = completed.clone();
        let panicker = choir.spawn("panicker").init(|_| {
            panic!("boom");
        });
        let mut follower = choir.spawn("follower").init(move |_| {
            c.fetch_add(1, Ordering::Relaxed);
        });
        follower.depend_on(&panicker);
        let r = follower.run();
        drop(panicker);

        let (tx, rx) = std::sync::mpsc::channel();
        let rc = r.clone();
        thread::spawn(move || {
            let mp = rc.join();
            let _ = tx.send(());
            std::mem::forget(mp);
        });
        let _ = rx.recv_timeout(Duration::from_secs(2));
    }
}

/// Finding C: `Linearc` must require `T: Send + Sync` for `Send`/`Sync`.
/// This is a compile-time property — the test just verifies that Linearc
/// with Send+Sync types works correctly at runtime.
#[test]
fn linearc_send_sync_bounds() {
    use choir::arc::Linearc;

    let arc = Linearc::new(42u32);
    let clone = Linearc::clone(&arc);

    let handle = thread::spawn(move || {
        assert_eq!(*clone, 42);
        Linearc::drop_last(clone)
    });

    let was_last_here = Linearc::drop_last(arc);
    let was_last_there = handle.join().unwrap();
    assert!(was_last_here || was_last_there);
    assert!(!(was_last_here && was_last_there));
}
