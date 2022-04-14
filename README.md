# Choir

[![Crates.io](https://img.shields.io/crates/v/choir.svg?label=choir)](https://crates.io/crates/choir)
[![Docs.rs](https://docs.rs/choir/badge.svg)](https://docs.rs/choir)
[![Build Status](https://github.com/kvark/choir/workflows/Check/badge.svg)](https://github.com/kvark/choir/actions)
![MSRV](https://img.shields.io/badge/rustc-1.56+-blue.svg)
[![codecov.io](https://codecov.io/gh/kvark/choir/branch/main/graph/badge.svg)](https://codecov.io/gh/kvark/choir)

Choir is a task orchestration framework. It helps you to organize all the CPU workflow in terms of tasks.

### Example:
```rust
let choir = choir::Choir::new();
let _worker = choir.add_worker("worker");
let task1 = choir.run_task(|| { println!("foo"); });
let task2 = choir.idle_task(|| { println!("bar"); });
task2.depend_on(&task1);
task2.run();
```

### Selling Pitch

What makes Choir _elegant_? Generally when we need to encode the semantics of "wait for dependencies", we think of some sort of a counter. Maybe an atomic, for the dependency number. When it reaches zero (or one), we schedule a task for execution. In _Choir_, the internal data for a task (i.e. the functor itself!) is placed in an `Arc`. Whenever we are able to extract it from the `Arc` (which means there are no other dependencies), we move it to a scheduling queue. I think Rust type system shows its best here.

You can add or remove workers at any time to balance the system load that may be running other applications at the same time.

### TODO:
  - loop detection
  - heavy use case


## Rough numbers

Machine: MBP 2016, 3.3 GHz Dual-Core Intel Core i7

function `run_task` (optimized): 237ns
function `run_task` (fallback): 401ns
"is alive": 34ns
"steal" task: 61ns
empty "execute": 37ns
dummy "unblock": 78ns
