// Copyright 2024 RustFS Team
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

//! Compiles the C shim over NVIDIA's C++ `libcuobjserver` and links it.
//!
//! `CUOBJSERVER_ROOT` points at a prefix holding `include/cuobjserver.h` and
//! `lib/libcuobjserver.so` when the library is not installed system-wide.

use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=csrc/cuobj_shim.cpp");
    println!("cargo:rerun-if-changed=csrc/cuobj_shim.h");
    println!("cargo:rerun-if-env-changed=CUOBJSERVER_ROOT");
    println!("cargo:rerun-if-env-changed=CUDA_HOME");

    let mut build = cc::Build::new();
    build
        .cpp(true)
        .file("csrc/cuobj_shim.cpp")
        .include("csrc")
        .flag_if_supported("-std=c++17")
        .warnings(true);
    if let Some(root) = std::env::var_os("CUOBJSERVER_ROOT").map(PathBuf::from) {
        build.include(root.join("include"));
        println!("cargo:rustc-link-search=native={}", root.join("lib").display());
    }
    // The CUDA toolkit also ships the headers and library (targets/<arch>-linux).
    let cuda = std::env::var_os("CUDA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/usr/local/cuda"));
    for target in ["sbsa-linux", "x86_64-linux"] {
        let inc = cuda.join("targets").join(target).join("include");
        let lib = cuda.join("targets").join(target).join("lib");
        if inc.join("cuobjserver.h").is_file() {
            build.include(&inc);
            println!("cargo:rustc-link-search=native={}", lib.display());
        }
    }
    build.compile("rustfs_cuobj_shim");
    println!("cargo:rustc-link-lib=cuobjserver");
    println!("cargo:rustc-link-lib=stdc++");
}
