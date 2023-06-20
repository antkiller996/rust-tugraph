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
};

fn main() {
    git_submodule_update_init();
    bindgen_tugraph_db();

    fail_on_empty_directory("tugraph-db");
    build_tugraph_db();
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

fn fail_on_empty_directory(name: &str) {
    if fs::read_dir(name).unwrap().count() == 0 {
        panic!(
            "The `{name}` directory is empty, did you forget to pull the submodules? Try `git submodule update --init --recursive`"
        );
    }
}

fn build_tugraph_db() {
    let target = env::var("TARGET").unwrap();
    if target != "x86_64-unknown-linux-gnu" {
        panic!(
            "Your target is {target}, but building tugraph-db is only tested in x86_64-unknown-linux-gnu"
        );
    }

    build_dep();

    println!("cargo:rerun-if-env-change=LGRAPH_C_COMPILER");
    println!("cargo:rerun-if-env-change=LGRAPH_CXX_COMPILER");

    // call cmake to configure and build liblgraph.so
    let c_compiler = env::var("LGRAPH_C_COMPILER").unwrap_or("/usr/bin/gcc".to_string());
    let cxx_compiler = env::var("LGRAPH_CXX_COMPILER").unwrap_or("/usr/bin/g++".to_string());

    let num_jobs = num_jobs(); // -j N
    let build_dir = out_dir().into_os_string().into_string().unwrap(); // -B ${build_dir}
    let bulid_type = build_type(); // -DCMAKE_BUILD_TYPE=${build_type}
    let dep_include_dir = deps_install_dir()
        .join("include")
        .into_os_string()
        .into_string()
        .unwrap(); // -DDEPS_INCLUDE_DIR=${dep_include_dir}
    let dep_lib_dir = deps_install_dir()
        .join("lib")
        .into_os_string()
        .into_string()
        .unwrap(); // -DDEPS_LIB_DIR=${dep_lib_dir}
    let dep_lib64_dir = deps_install_dir()
        .join("lib64")
        .into_os_string()
        .into_string()
        .unwrap(); // -DDEPS_LIB64_DIR=${dep_lib64_dir}
    run_cmd(
        Command::new("/bin/bash").args([
            "-c",
            format!(
                "
        cmake -S tugraph-db -B {build_dir} \
            -DCMAKE_CXX_COMPILER={cxx_compiler} \
            -DCMAKE_C_COMPILER={c_compiler} \
            -DCMAKE_BUILD_TYPE={bulid_type} \
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
            -DDEPS_INCLUDE_DIR={dep_include_dir} \
            -DDEPS_LIB_DIR={dep_lib_dir} \
            -DDEPS_LIB64_DIR={dep_lib64_dir} \
        && cmake --build {build_dir} -j {num_jobs} --target lgraph"
            )
            .as_str(),
        ]),
    );
    println!(
        "cargo:rustc-link-search=native={}",
        out_dir().join("output").into_os_string().into_string().unwrap()
    );
    // dynamic link liblgraph.so
    println!("cargo:rustc-link-lib=dylib=lgraph");
}

fn build_dep() {
    let num_jobs = num_jobs();
    let out_dir = deps_out_dir().into_os_string().into_string().unwrap();
    run_cmd(Command::new("/bin/bash").args([
        "-c",
        format!("SKIP_WEB=1 tugraph-db/deps/build_deps.sh -j{num_jobs} -o {out_dir}").as_str(),
    ]));
}

fn num_jobs() -> String {
    env::var("NUM_JOBS").unwrap_or_else(|_| "1".to_string())
}

fn out_dir() -> PathBuf {
    PathBuf::from(env::var("OUT_DIR").unwrap())
}

fn deps_out_dir() -> PathBuf {
    out_dir().join("deps")
}

fn deps_install_dir() -> PathBuf {
    deps_out_dir().join("install")
}

fn build_type() -> String {
    let profile = env::var("PROFILE").unwrap_or_else(|_| "debug".to_string());
    profile[0..1].to_uppercase() + &profile[1..]
}
