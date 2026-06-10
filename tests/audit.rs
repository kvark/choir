use std::{thread, time::Duration};

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
    let handle = thread::spawn(move || {
        let mp = b_clone.join();
        let _ = tx.send(());
        std::mem::forget(mp);
    });

    rx.recv_timeout(Duration::from_secs(5))
        .expect("B must not hang when A panics");
    handle.join().unwrap();
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
    let handle = thread::spawn(move || {
        let mp = c_clone.join();
        let _ = tx.send(());
        std::mem::forget(mp);
    });

    rx.recv_timeout(Duration::from_secs(5))
        .expect("C must not hang when A panics transitively");
    handle.join().unwrap();
}

/// Finding B: `finish()` must not crash if the continuation was already
/// taken (e.g., by `flush_notifier` during panic handling).
/// Each iteration uses a fresh choir because panicking workers die.
#[test]
fn finish_tolerates_taken_continuation() {
    let _ = env_logger::try_init();

    let iterations = if cfg!(miri) { 2 } else { 10 };
    for _ in 0..iterations {
        let choir = choir::Choir::new();
        let _w1 = choir.add_worker("W1");
        let _w2 = choir.add_worker("W2");

        let panicker = choir.spawn("panicker").init(|_| {
            panic!("boom");
        });
        let mut follower = choir.spawn("follower").init(|_| {});
        follower.depend_on(&panicker);
        let r = follower.run();
        drop(panicker);

        let (tx, rx) = std::sync::mpsc::channel();
        let rc = r.clone();
        let handle = thread::spawn(move || {
            let mp = rc.join();
            let _ = tx.send(());
            std::mem::forget(mp);
        });
        let _ = rx.recv_timeout(Duration::from_secs(2));
        handle.join().unwrap();
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

/// A timed-out `join_debug` must deregister its stack condvar before
/// unwinding. If it doesn't, the task finishing later notifies a dangling
/// pointer into the dead stack frame (use-after-free, caught by Miri).
#[test]
fn join_debug_timeout_no_dangling_waiter() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _w = choir.add_worker("W");

    // The task blocks until we allow it to finish.
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let task = choir
        .spawn("slow")
        .init(move |_| {
            let _ = rx.recv();
        })
        .run();

    // Join with a tiny timeout on a helper thread; the timeout panic is
    // expected and contained to that thread.
    let task_clone = task.clone();
    let join_result = thread::spawn(move || {
        task_clone.join_debug(Duration::from_millis(50));
    })
    .join();
    assert!(join_result.is_err(), "join_debug must panic on timeout");

    // Let the task finish: `finish` walks waiting_threads and must not
    // touch the timed-out joiner's (now destroyed) condvar.
    tx.send(()).unwrap();
    task.join();
}
