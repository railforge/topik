# topik

[![CI](https://github.com/railforge/topik/workflows/CI/badge.svg)](https://github.com/railforge/topik/actions)
[![Crates.io](https://img.shields.io/crates/v/topik.svg)](https://crates.io/crates/topik)
[![docs.rs](https://docs.rs/topik/badge.svg)](https://docs.rs/topik)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

<p align="center">
  <img src="https://raw.githubusercontent.com/railforge/topik/main/assets/topik-logo.png" alt="topik logo" width="200"/>
</p>

You know your topics. Now your compiler can too.

Topik brings compile-time type safety to pub/sub. Define your messaging infrastructure as a versioned Rust crate. One source of truth that grows with your system. New topics, new protocols, new services. Every change tracked in code, every type checked at compile time.

## Crates

| Crate | Purpose |
|-------|---------|
| [`topik`](topik/) | The main crate. Add this to your `Cargo.toml`. |
| [`topik-core`](topik-core/) | Core traits. For custom transport or encoding implementations only. |
| [`topik-macros`](topik-macros/) | Proc macros. Crate internal, do not depend on this directly. |

## Getting started

See [`topik/README.md`](topik/README.md) for installation, usage, and examples.

## Contributing

All contributions are welcome.

Most new features belong in `topik`. If you are implementing a new transport, depend on `topik-core` and implement the `Transport` trait. If you are implementing a new encoding, implement the `Encoding` trait.

Open an issue first for larger changes. For small fixes and improvements a PR is fine directly.

To run the full test suite:

```bash
cargo test --workspace
```

To check for warnings:

```bash
cargo clippy --workspace -- -D warnings
```

## License

MIT see [LICENSE](LICENSE).
