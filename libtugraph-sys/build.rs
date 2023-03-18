// Copyright 2023 antkiller
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
};

fn main() {
    git_submodule_update_init();
    bindgen_tugraph_db();

    let num_jobs = env::var("NUM_JOBS").unwrap_or("2".to_string());
    if !try_to_find_and_copy_liblgraph() {
        fail_on_empty_directory("tugraph-db");
        build_tugraph_db(&num_jobs);
    }
}

fn git_submodule_update_init() {
    if !Path::new("tugraph-db/LICENSE").exists()
        || !Path::new("tugraph-db/deps/antlr4/LICENSE.txt").exists()
    {
        run_cmd(Command::new("git").args(["submodule", "update", "--init", "--recursive"]));
    }
}

fn run_cmd(cmd: &mut Command) {
    println!("Running command: $ {cmd:?}");
    match cmd.status().map(|s| (s.success(), s.code())) {
        Ok((true, _)) => (),
        Ok((false, Some(c))) => panic!("Command failed with error code {c}"),
        Ok((false, None)) => panic!("Command got killed"),
        Err(e) => panic!("Command failed with error: {e}"),
    }
}

fn bindgen_tugraph_db() {
    let c_header = tugraph_db_include_dir()
        .join("lgraph/c.h")
        .into_os_string()
        .into_string()
        .unwrap();
    println!("cargo:rerun-if-changed={}", c_header);

    let bindings = bindgen::Builder::default()
        .header(c_header)
        .derive_debug(false)
        .ctypes_prefix("libc")
        .size_t_is_usize(true)
        .generate()
        .expect("unable to generate tugraph-db bindings");
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("unable to write tugraph-db bindings");
}

fn tugraph_db_include_dir() -> PathBuf {
    let dir = env::var("TUGRAPH_DB_INCLUDE_DIR").unwrap_or("tugraph-db/include".to_string());
    PathBuf::from(dir)
}

fn try_to_find_and_copy_liblgraph() -> bool {
    println!("cargo:rerun-if-env-changed=LGRAPH_COMPILE");
    if let Ok(v) = env::var("LGRAPH_COMPILE") {
        if v.to_lowercase() == "true" || v == "1" {
            return false;
        }
    }

    println!("cargo:rerun-if-env-changed=LGRAPH_LIB_DIR");
    println!("cargo:rerun-if-env-changed=LGRAPH_STATIC");

    if let Ok(src_lib_dir) = env::var("LGRAPH_LIB_DIR") {
        let src_lib_dir = PathBuf::from_str(&src_lib_dir).unwrap();
        copy_library_to_out_dir(
            src_lib_dir.join("liblgraph.so"),
            out_dir().join("liblgraph.so"),
        );
        let mode = match env::var_os("LGRAPH_STATIC") {
            Some(_) => "static",
            None => "dylib",
        };
        println!("cargo:rustc-link-search=native={}", out_dir().display());
        println!("cargo:rustc-link-lib={}=lgraph", mode);
        return true;
    }
    false
}

fn fail_on_empty_directory(name: &str) {
    if fs::read_dir(name).unwrap().count() == 0 {
        panic!(
            "The `{name}` directory is empty, did you forget to pull the submodules? Try `git submodule update --init --recursive`"
        );
    }
}

fn build_tugraph_db(num_jobs: &str) {
    let target = env::var("TARGET").unwrap();
    if target != "x86_64-unknown-linux-gnu" {
        panic!(
            "Your target is {target}, but building tugraph-db is only tested in x86_64-unknown-linux-gnu"
        );
    }

    build_dep(num_jobs);

    println!("cargo:rerun-if-env-change=LGRAPH_C_COMPILER");
    println!("cargo:rerun-if-env-change=LGRAPH_CXX_COMPILER");

    // call cmake to configure and build liblgraph.so
    let c_compiler = env::var("LGRAPH_C_COMPILER").unwrap_or("/usr/bin/gcc".to_string());
    let cxx_compiler = env::var("LGRAPH_CXX_COMPILER").unwrap_or("/usr/bin/g++".to_string());

    println!("cargo:rerun-if-env-change=LGRAPH_RECOMPILE");
    if let Ok(recompile) = env::var("LGRAPH_RECOMPILE") {
        if recompile.to_lowercase() == "true" || recompile == "1" {
            run_cmd(Command::new("/bin/bash").args(["-c", "rm -rf tugraph-db/build"]));
        }
    }
    run_cmd(
        Command::new("/bin/bash").args([
            "-c",
            format!(
                "
        cd tugraph-db && mkdir -p build && cd build \
        && cmake .. -DCMAKE_CXX_COMPILER={cxx_compiler} \
                    -DCMAKE_C_COMPILER={c_compiler} \
                    -DCMAKE_BUILD_TYPE=Debug \
                    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        && make -j{num_jobs} lgraph && cd ../.."
            )
            .as_str(),
        ]),
    );
    copy_library_to_out_dir(
        "tugraph-db/build/output/liblgraph.so",
        out_dir().join("liblgraph.so"),
    );
    println!("cargo:rustc-link-search=native={}", out_dir().display());
    // dynamic link liblgraph.so
    println!("cargo:rustc-link-lib=dylib=lgraph");
}

fn build_dep(num_jobs: &str) {
    run_cmd(Command::new("/bin/bash").args([
        "-c",
        format!("cd tugraph-db && SKIP_WEB=1 deps/build_deps.sh -j{num_jobs} && cd ..").as_str(),
    ]));
}

fn out_dir() -> PathBuf {
    PathBuf::from(env::var("OUT_DIR").unwrap())
}

fn copy_library_to_out_dir<S: AsRef<Path>, D: AsRef<Path>>(src: S, dst: D) {
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html#dynamic-library-paths
    // Paths included from cargo:rustc-link-search outside of the `target/` directory are removed
    // when adding into dynmaic library search path.
    // lgraph_out_dir return the path within `target/` directory
    std::fs::copy(src.as_ref(), dst.as_ref())
        .unwrap_or_else(|_| panic!("failed to copy {:?}", src.as_ref()));
}
