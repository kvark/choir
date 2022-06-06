fn main() {
    let count = 10_000_000;
    let multi = true;
    let mut choir = choir::Choir::new();
    let _workers = (0..2)
        .map(|i| choir.add_worker(&format!("worker-{}", i)))
        .collect::<Vec<_>>();
    if multi {
        choir.add_multi_task(count, |_| {});
    } else {
        for _ in 0..count {
            choir.add_task(|| {});
        }
    }
    choir.wait_idle();
}
