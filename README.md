rust-tugraph
=====================
[![CI](https://github.com/antkiller996/rust-tugraph/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/antkiller996/rust-tugraph/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/tugraph)](tugraph)
[![docs.rs](https://img.shields.io/docsrs/tugraph/latest)](https://docs.rs/tugraph)
[![license](https://img.shields.io/crates/l/tugraph)](https://github.com/antkiller996/rust-tugraph/blob/master/LICENSE)
![rust 1.68.0 required](https://img.shields.io/badge/rust-1.68.0-blue.svg?label=MSRV)

# Requirements
- tugraph-db Build Toolchains
    - CMake(>=3.15)
    - g++(>=8.2)
- tugraph-db Dependencies
    - See tugraph-db [Dockerfile]


# Contributing
Any feedback and pull requests are welcome! If some apis are not flexiable, let me know and I'll relax constraints. If some public types or apis don't conform well to [Rust API Guidelines Checklists], open issues or send pull requests.



# Usage
Now this binding is dynamically linked with liblgraph.so built from tugraph-db. It aims to port rust apis to write rust procedure(a.k.a tugraph plugins). If you want to statically link with liblgraph.a, let me know.

`rust-tugraph` depends on `libtugraph-sys` which is a unsafe wrapper of tugraph c++ apis. `libtugraph-sys` uses a build script `build.rs` to build liblgraph.so, which delegates to cmake and other build c++ build essentials. The most important part is to choose g++/gcc compiler, and the build script exports two environment vars `LGRAPH_CXX_COMPILER` and `LGRAPH_C_COMPILER`.

```bash
LGRAPH_CXX_COMPILER=/usr/local/bin/g++ \
LGRAPH_C_COMPILER=/usr/local/bin/gcc \
cargo {build,run,test} [options] {target}
```

If you want to write rust procedure, crate [tugraph-plugin-util] helps you a lot.
> **NOTE**: After TuGraph [b8dcaac](https://github.com/TuGraph-family/tugraph-db/commit/b8dcaac1b70e83b191c4644f182d8d92b26bfef4) version, it supports writing plugin in rust language 



[Dockerfile]: https://github.com/TuGraph-family/tugraph-db/tree/master/ci/images
[Rust API Guidelines Checklists]: https://rust-lang.github.io/api-guidelines/checklist.html
[tugraph-plugin-util]: plugin-util/README.md
