fn main() {
    let mut choir = choir::Choir::new();
    let _worker = choir.add_worker("worker");
    for _ in 0..10000000 {
        choir.run_task(|| {});
    }
    choir.wait_idle();
}
