fn main() {
    let mut choir = choir::Choir::new();
    let _workers = (0..2)
        .map(|i| choir.add_worker(&format!("worker-{}", i)))
        .collect::<Vec<_>>();
    for _ in 0..1_000_000 {
        choir.spawn("").init(|_| {});
    }
    choir.wait_idle();
}
