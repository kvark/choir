#[cfg(feature = "loom")]
use loom::sync;
#[cfg(not(feature = "loom"))]
use std::sync;

use self::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[cfg(not(loom))]
mod loom {
    pub fn model<F: FnOnce()>(fun: F) {
        fun();
    }
}

#[test]
fn parallel() {
    let _ = env_logger::try_init();
    loom::model(|| {
        let mut choir = choir::Choir::new();
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
    })
}

#[test]
fn sequential() {
    let _ = env_logger::try_init();
    loom::model(|| {
        let mut choir = choir::Choir::new();
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
    })
}

#[test]
fn zero_count() {
    loom::model(|| {
        let mut choir = choir::Choir::new();
        let _worker1 = choir.add_worker("A");
        choir.spawn("").init_multi(0, |_, _| {}).run().join();
    })
}

#[test]
fn multi_sum() {
    let _ = env_logger::try_init();
    loom::model(|| {
        let mut choir = choir::Choir::new();
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
    })
}

#[test]
fn iter_xor() {
    let _ = env_logger::try_init();
    loom::model(|| {
        let mut choir = choir::Choir::new();
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
    })
}

#[test]
fn proxy() {
    let _ = env_logger::try_init();
    loom::model(|| {
        let mut choir = choir::Choir::new();
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
    })
}
