# topik-core

[![Crates.io](https://img.shields.io/crates/v/topik-core.svg)](https://crates.io/crates/topik-core)
[![docs.rs](https://docs.rs/topik-core/badge.svg)](https://docs.rs/topik-core)
[![MSRV](https://img.shields.io/badge/rustc-1.88+-blue.svg)](https://blog.rust-lang.org/2025/05/15/Rust-1.88.0.html)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Core traits for [topik](https://crates.io/crates/topik).

If you are building a Rust application using topik, add `topik` to your `Cargo.toml` instead. This crate is for implementors only.

## When to depend on this crate

Add `topik-core` as a dependency if you are implementing:

- A custom transport: implement the `Transport` trait
- A custom encoding: implement the `Encoding` trait
- A custom bool representation: implement the `BoolRepr` trait

## Traits

| Trait | Purpose |
|-------|---------|
| `Topic` | Derived on topic structs. The core user-facing trait. |
| `TopicWire` | Internal. Used by backends to render and parse topic strings. |
| `Transport` | Implement this to add a new broker backend. |
| `Encoding<T>` | Implement this to add a new payload encoding. |
| `BoolRepr` | Implement this to define custom bool wire representations. |
| `Segment` | Implement this to use a custom type as a topic segment. |

## License

MIT see [LICENSE](../../LICENSE).
