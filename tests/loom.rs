//! Loom-based concurrency tests for choir.
//!
//! Run with: `RUSTFLAGS='--cfg loom' cargo test --test loom --release`

#![cfg(loom)]

use choir::arc::Linearc;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::Arc;
use loom::thread;

/// Two threads race to drop a Linearc. Exactly one of them should
/// cause deallocation; the other must be a no-op.
#[test]
fn linearc_concurrent_drop() {
    loom::model(|| {
        let a = Linearc::new(42u32);
        let b = Linearc::clone(&a);

        let t = thread::spawn(move || {
            drop(b);
        });

        drop(a);
        t.join().unwrap();
    });
}

/// Two threads race: one calls `into_inner`, the other drops.
/// Exactly one of them should get the inner value.
#[test]
fn linearc_into_inner_race() {
    loom::model(|| {
        let a = Linearc::new(7u32);
        let b = Linearc::clone(&a);

        let got_value = Arc::new(AtomicUsize::new(0));
        let gv = got_value.clone();

        let t = thread::spawn(move || {
            if Linearc::into_inner(b).is_some() {
                gv.fetch_add(1, Ordering::SeqCst);
            }
        });

        if Linearc::into_inner(a).is_some() {
            got_value.fetch_add(1, Ordering::SeqCst);
        }

        t.join().unwrap();
        // Exactly one thread should have extracted the value.
        assert_eq!(got_value.load(Ordering::SeqCst), 1);
    });
}

/// Three clones, three threads dropping. Verify no double-free.
#[test]
fn linearc_triple_drop() {
    loom::model(|| {
        let a = Linearc::new(99u32);
        let b = Linearc::clone(&a);
        let c = Linearc::clone(&a);

        let t1 = thread::spawn(move || drop(b));
        let t2 = thread::spawn(move || drop(c));
        drop(a);

        t1.join().unwrap();
        t2.join().unwrap();
    });
}

/// `drop_last` race: two threads call `drop_last` on clones.
/// Exactly one should return `true`.
#[test]
fn linearc_drop_last_race() {
    loom::model(|| {
        let a = Linearc::new(0u32);
        let b = Linearc::clone(&a);

        let result = Arc::new(AtomicUsize::new(0));
        let r = result.clone();

        let t = thread::spawn(move || {
            if Linearc::drop_last(b) {
                r.fetch_add(1, Ordering::SeqCst);
            }
        });

        if Linearc::drop_last(a) {
            result.fetch_add(1, Ordering::SeqCst);
        }

        t.join().unwrap();
        assert_eq!(result.load(Ordering::SeqCst), 1);
    });
}

/// Single task: spawn, execute on a worker, join.
/// Tests the full schedule → steal → execute → finish → unpark path.
#[test]
fn single_task() {
    loom::model(|| {
        let choir = choir::Choir::new();
        let _w = choir.add_worker("W");
        let flag = Arc::new(AtomicUsize::new(0));
        let f = flag.clone();
        choir
            .spawn("t")
            .init(move |_| {
                f.store(1, Ordering::Release);
            })
            .run()
            .join();
        assert_eq!(flag.load(Ordering::Acquire), 1);
    });
}

/// Dummy task with a dependency: B depends on A.
/// Verifies the Linearc-based dependency resolution.
#[test]
fn dependency() {
    loom::model(|| {
        let choir = choir::Choir::new();
        let _w = choir.add_worker("W");

        let mut b = choir.spawn("B").init_dummy();
        let a = choir.spawn("A").init_dummy();
        b.depend_on(&a);
        drop(a); // schedule A via IdleTask::drop
        b.run().join();
    });
}
