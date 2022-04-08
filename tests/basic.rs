use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[test]
fn basic() {
    let mut choir = choir::Choir::new();
    let _singer1 = choir.add_singer("A");
    let _signer2 = choir.add_singer("B");
    let value = Arc::new(AtomicUsize::new(0));
    let n = 100;
    for _ in 0..n {
        let v = Arc::clone(&value);
        choir.sing(move || {
            v.fetch_add(1, Ordering::AcqRel);
        });
    }
    choir.wait_idle();
    assert_eq!(value.load(Ordering::Acquire), n);
}
