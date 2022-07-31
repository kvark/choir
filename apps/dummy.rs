fn main() {
    let mut choir = choir::Choir::new();
    let _workers = (0..2)
        .map(|i| choir.add_worker(&format!("worker-{}", i)))
        .collect::<Vec<_>>();
    let mut end = choir.spawn("end").init_dummy();
    for _ in 0..1_000_000 {
        let task = choir.spawn("").init_dummy();
        end.depend_on(&task);
    }
    end.run().join();
}
