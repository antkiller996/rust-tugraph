# A echo plugin example
This is a example of tugraph rust procedure, which accept request return request with number of vertices.

# Usage

## Compile to dynamic library
Run following command to compile, you'll get a dynamic library named `libplugin_example.so` in **target/release**.

```bash
cargo build --release -p plugin-example
```

## How to use plugin
See tugraph [procedure](https://github.com/TuGraph-family/tugraph-db/blob/master/doc/zh-CN/source/5.developer-manual/6.interface/3.procedure/1.procedure.md#22加载存储过程)