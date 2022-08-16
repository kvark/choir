# Choir

[![Crates.io](https://img.shields.io/crates/v/choir.svg?label=choir)](https://crates.io/crates/choir)
[![Docs.rs](https://docs.rs/choir/badge.svg)](https://docs.rs/choir)
[![Build Status](https://github.com/kvark/choir/workflows/Check/badge.svg)](https://github.com/kvark/choir/actions)
![MSRV](https://img.shields.io/badge/rustc-1.56+-blue.svg)
[![codecov.io](https://codecov.io/gh/kvark/choir/branch/main/graph/badge.svg)](https://codecov.io/gh/kvark/choir)

Choir is a task orchestration framework. It helps you to organize all the CPU workflow in terms of tasks.

### Example:
```rust
let mut choir = choir::Choir::new();
let _worker = choir.add_worker("worker");
let task1 = choir.spawn("foo").init_dummy().run();
let mut task2 = choir.spawn("bar").init(|_| { println!("bar"); });
task2.depend_on(&task1);
task2.run().join();
```

### Selling Pitch

What makes Choir _elegant_? Generally when we need to encode the semantics of "wait for dependencies", we think of some sort of a counter. Maybe an atomic, for the dependency number. When it reaches zero (or one), we schedule a task for execution. In _Choir_, the internal data for a task (i.e. the functor itself!) is placed in an `Arc`. Whenever we are able to extract it from the `Arc` (which means there are no other dependencies), we move it to a scheduling queue. I think Rust type system shows its best here.

Note: it turns out `Arc` doesn't fully support such a "linear" usage as required here, and it's impossible to control where the last reference gets destructed (without logic in `drop()`). For this reason, we introduce our own `Linearc` to be used internally.

You can also add or remove workers at any time to balance the system load, which may be running other applications at the same time.

## API

General workflow is about creating tasks and setting up dependencies between them. There is a few different kinds of tasks:
  - single-run tasks, initialized with `init()` and represented as `FnOnce()`
  - dummy tasks, initialized with `init_dummy()`, and having no function body
  - multi-run tasks, executed for every index in a range, represented as `Fn(SubIndex)`, and initialized with `init_multi()`
  - iteration tasks, executed for every item produced by an iterator, represented as `Fn(T)`, and initialized with `init_iter()`

Just calling `run()` is done automatically on `IdleTask::drop()` if not called explicitly.
This object also allows adding dependencies before scheduling the task. The running task can be also used as a dependency for others.

Note that all tasks are pre-empted at the `Fn()` execution boundary. Thus, for example, a long-running multi task will be pre-empted by any incoming single-run tasks.

### TODO:
  - loop detection
  - heavy use case
  - loom testing

## Rough numbers

Machine: MBP 2016, 3.3 GHz Dual-Core Intel Core i7

- functions `spawn()+init()` (optimized): 237ns
- "steal" task: 61ns
- empty "execute": 37ns
- dummy "unblock": 78ns

Executing 100k empty tasks:
- individually: 28ms
- as a multi-task: 6ms

## Testing

Shuttle run:
```bash
RUSTFLAGS="--cfg shuttle" cargo test >shuttle.txt
```
