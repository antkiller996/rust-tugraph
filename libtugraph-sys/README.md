# TuGraph-DB bindings
Low-level bindings to [tugraph-db] C API.

It is highly inspired by [librocksdb-sys].

## Version
The libtugraph-sys version number follows as librocksdb-sys which is in format `X.Y.Z+TX.TY.TZ`, where
`X.Y.Z` is the version of this crate and follows SemVer conventions, while `TX.TY.TZ` is the version of the
bundled tugraph-db. 


[tugraph-db]: https://github.com/TuGraph-family/tugraph-db
[librocksdb-sys]: https://crates.io/crates/librocksdb-sys