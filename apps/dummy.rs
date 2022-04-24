fn main() {
    let count = 10_000_000;
    let multi = true;
    let mut choir = choir::Choir::new();
    let _workers = (0..2)
        .map(|i| choir.add_worker(&format!("worker-{}", i)))
        .collect::<Vec<_>>();
    let end = if multi {
        let mut end = choir.spawn("end").init_multi(count, |_| {});
    } else {
        let mut end = choir.spawn("end").init_dummy();
        for _ in 0..count {
            let task = choir.spawn("").init_dummy();
            end.depend_on(&task);
        }
        end
    }
    end.run().join();
}
