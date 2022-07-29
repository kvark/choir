use rand::Rng as _;
use std::{mem, ptr, slice};

type Value = i64;

static mut CHOIR: *const choir::Choir = ptr::null();

#[derive(Clone, Copy)]
struct Array {
    ptr: *mut Value,
    count: usize,
}

// Not sure why Rust doesn't consider pointers movable?
unsafe impl Send for Array {}

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

unsafe fn split(data: Array) -> (usize, usize) {
    let mid = *data.ptr;
    let mut i = 1;
    let mut j = data.count - 1;
    'outer: while i < j {
        while *data.ptr.add(i) <= mid {
            i += 1;
            if i == j {
                break 'outer;
            }
        }
        while *data.ptr.add(j) > mid {
            j -= 1;
            if i == j {
                break 'outer;
            }
        }
        mem::swap(&mut *data.ptr.add(i), &mut *data.ptr.add(j));
        i += 1;
        j -= 1;
    }
    // guarantee that the element in the middle can be excluded
    if *data.ptr.add(j) <= mid {
        mem::swap(&mut *data.ptr, &mut *data.ptr.add(j));
        (j, j + 1)
    } else {
        (j, j)
    }
}

unsafe fn qsort(data: Array) {
    if data.count > 5 {
        let (left_end, right_start) = split(data);
        let left = Array {
            ptr: data.ptr,
            count: left_end,
        };
        let right = Array {
            ptr: data.ptr.add(right_start),
            count: data.count - right_start,
        };
        (*CHOIR).spawn("left").init(move |_| qsort(left));
        (*CHOIR).spawn("right").init(move |_| qsort(right));
    } else if data.count > 1 {
        insertion_sort(slice::from_raw_parts_mut(data.ptr, data.count))
    }
}

fn main() {
    const USE_TASKS: bool = true;
    const COUNT: usize = 10000000;
    let mut data = {
        let mut rng = rand::thread_rng();
        (0..COUNT).map(|_| rng.gen()).collect::<Vec<Value>>()
    };

    if USE_TASKS {
        let data_raw = Array {
            ptr: data.as_mut_ptr(),
            count: COUNT,
        };
        let mut choir = choir::Choir::new();
        let _worker1 = choir.add_worker("worker1");
        let _worker2 = choir.add_worker("worker2");
        unsafe {
            CHOIR = &choir as *const _;
        }
        choir
            .spawn("main")
            .init(move |_| unsafe { qsort(data_raw) });
        choir.wait_idle();
        unsafe {
            CHOIR = ptr::null();
        }
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
