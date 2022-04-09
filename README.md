# Choir

[![Crates.io](https://img.shields.io/crates/v/choir.svg?label=choir)](https://crates.io/crates/choir)
[![Docs.rs](https://docs.rs/choir/badge.svg)](https://docs.rs/choir)
[![Build Status](https://github.com/kvark/choir/workflows/pipeline/badge.svg)](https://github.com/kvark/choir/actions)
![MSRV](https://img.shields.io/badge/rustc-1.56+-blue.svg)

Choir is a task orchestration framework. It helps you to organize all the CPU workflow in terms of tasks.

_Principles_ are simple: no unsafe code, and minimize the locking.

General _use-case_ is: you create one task, then another, then establish some dependencies between them, then go. The task system is always alive, always chewing through work, or sleeping waiting for it.

What makes Choir _elegant_? Generally when we need to encode the semantics of "wait for dependencies", we think of some sort of a counter. Maybe an atomic, for the dependency number. When it reaches zero (or one), we schedule a task for execution. In _Choir_, the internal data for a task (i.e. the functor itself!) is placed in an `Arc`. Whenever we are able to extract it from the `Arc` (which means there are no other dependencies), we move it to a scheduling queue. I think Rust type system shows its best here.

**TODO**:
  - code coverage
  - benchmarking
  - loop detection
  - heavy use case
  - reconsider goofy names?
