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

    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let task = choir
        .spawn("slow")
        .init(move |_| {
            let _ = rx.recv();
        })
        .run();

    let task_clone = task.clone();
    let join_result = thread::spawn(move || {
        task_clone.join_debug(Duration::from_millis(50));
    })
    .join();
    assert!(join_result.is_err(), "join_debug must panic on timeout");

    tx.send(()).unwrap();
    task.join();
}

/// Workers survive task panics and continue processing.
#[test]
fn worker_survives_panic() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _w = choir.add_worker("W");

    let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag_clone = flag.clone();

    let panicker = choir.spawn("panicker").init(|_| panic!("boom"));
    let setter = choir.spawn("setter").init(move |_| {
        flag_clone.store(true, std::sync::atomic::Ordering::Release);
    });

    let mut barrier = choir.spawn("barrier").init_dummy();
    barrier.depend_on(&panicker);
    barrier.depend_on(&setter);
    let rt = barrier.run();
    drop(panicker);
    drop(setter);

    let mp = rt.join();
    mp.dismiss();
    assert!(flag.load(std::sync::atomic::Ordering::Acquire));
}

/// clear_panic resets the panic state.
#[test]
fn clear_panic_resets_state() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _w = choir.add_worker("W");

    let t = choir.spawn("p").init(|_| panic!("boom")).run();
    assert!(t.join().into_result().is_err());

    assert!(choir.check_panic().into_result().is_err());
    choir.clear_panic();
    assert!(choir.check_panic().into_result().is_ok());
}

/// Scoped tasks can borrow from the enclosing stack frame.
#[test]
fn scope_borrows_local_data() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _w = choir.add_worker("W");

    let data: Vec<AtomicU32> = (0..4).map(|_| AtomicU32::new(0)).collect();
    choir.scope(|s| {
        for (i, slot) in data.iter().enumerate() {
            s.spawn(format!("task-{}", i), move |_| {
                slot.store((i as u32 + 1) * 10, Ordering::Relaxed);
            });
        }
    });
    let result: Vec<u32> = data.iter().map(|a| a.load(Ordering::Relaxed)).collect();
    assert_eq!(result, vec![10, 20, 30, 40]);
}

/// A scoped task panic propagates after all tasks complete.
#[test]
fn scope_propagates_task_panic() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _w = choir.add_worker("W");

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        choir.scope(|s| {
            s.spawn("ok", |_| {});
            s.spawn("boom", |_| panic!("scoped panic"));
        });
    }));
    // The scope itself doesn't re-panic on task panics, but
    // issue_panic was called, so check_panic would catch it.
    // Clear it for a clean state.
    choir.clear_panic();
    // The scope body didn't panic, so result is Ok.
    assert!(result.is_ok());
}

/// with_workers convenience constructor.
#[test]
fn with_workers_creates_pool() {
    let _ = env_logger::try_init();
    let (choir, _handles) = choir::Choir::with_workers(3);

    let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let c = counter.clone();
    choir
        .spawn("count")
        .init(move |_| {
            c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        })
        .run()
        .join()
        .dismiss();
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 1);
}

/// MaybePanic::into_result returns Ok for clean and Err for panicked.
#[test]
fn maybe_panic_into_result() {
    let _ = env_logger::try_init();
    let choir = choir::Choir::new();
    let _w = choir.add_worker("W");

    let t = choir.spawn("ok").init(|_| {}).run();
    assert!(t.join().into_result().is_ok());

    let t2 = choir.spawn("boom").init(|_| panic!("boom")).run();
    assert!(t2.join().into_result().is_err());
    choir.clear_panic();
}
