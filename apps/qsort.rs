use rand::Rng as _;
use std::{mem, ptr, slice};

type Value = i64;

fn insertion_sort(data: &mut [Value]) {
    for i in 1..data.len() {
        let v = data[i];
        let mut j = i;
        while j != 0 {
            let w = data[j - 1];
            if w <= v {
                data[j] = v;
                break;
            } else {
                data[j] = w;
                j -= 1;
            }
        }
    }
}

fn split(data: &mut [Value]) -> (&mut [Value], &mut [Value]) {
    let mid = data[0];
    let mut i = 1;
    let mut j = data.len() - 1;
    'outer: while i < j {
        while data[i] <= mid {
            i += 1;
            if i == j {
                break 'outer;
            }
        }
        while data[j] > mid {
            j -= 1;
            if i == j {
                break 'outer;
            }
        }
        unsafe {
            ptr::swap(&mut data[i], &mut data[j]);
        }
        i += 1;
        j -= 1;
    }

    let (left, right) = data.split_at_mut(j);
    // guarantee that the element in the middle can be excluded
    if right[0] <= mid {
        mem::swap(&mut left[0], &mut right[0]);
        (left, right.split_first_mut().unwrap().1)
    } else {
        (left, right)
    }
}

fn qsort(data: *mut Value, size: usize, context: choir::ExecutionContext) {
    if size > 5 {
        let (left, right) = split(unsafe { slice::from_raw_parts_mut(data, size) });
        context
            .fork("left")
            .init(move |ec| qsort(left.as_mut_ptr(), left.len(), ec));
        context
            .fork("right")
            .init(move |ec| qsort(right.as_mut_ptr(), right.len(), ec));
    } else if size > 1 {
        insertion_sort(unsafe { slice::from_raw_parts_mut(data, size) })
    }
}

fn main() {
    const USE_TASKS: bool = true;
    const COUNT: usize = 10000000;
    env_logger::init();

    let mut data = {
        let mut rng = rand::thread_rng();
        (0..COUNT).map(|_| rng.gen()).collect::<Vec<Value>>()
    };

    if USE_TASKS {
        let choir = choir::Choir::new();
        let _worker1 = choir.add_worker("worker1");
        let _worker2 = choir.add_worker("worker2");
        let data_ptr = data.as_mut_ptr() as usize;
        choir
            .spawn("main")
            .init(move |ec| qsort(data_ptr as *mut Value, COUNT, ec))
            .run_attached();
    } else {
        insertion_sort(&mut data);
    }

    // check if sorted
    if let Some(position) = data.iter().zip(data[1..].iter()).position(|(a, b)| a > b) {
        panic!(
            "position {}: {} > {}",
            position,
            data[position],
            data[position + 1]
        );
    }
}
