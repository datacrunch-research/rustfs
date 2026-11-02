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

fn main() -> shadow_rs::SdResult<()> {
    shadow_rs::ShadowBuilder::builder().build()?;
    build_rdma_shim();
    Ok(())
}

/// Compiles the libibverbs/librdmacm shim only for `--features rdma` builds, so default
/// builds never require the RDMA headers or libraries.
fn build_rdma_shim() {
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_RDMA");
    if std::env::var_os("CARGO_FEATURE_RDMA").is_none() {
        return;
    }
    println!("cargo:rerun-if-changed=csrc/rdma_shim.c");
    println!("cargo:rerun-if-changed=csrc/rdma_shim.h");
    cc::Build::new()
        .file("csrc/rdma_shim.c")
        .include("csrc")
        .warnings(true)
        .flag_if_supported("-Wextra")
        .compile("rustfs_rdma_shim");
    println!("cargo:rustc-link-lib=ibverbs");
    println!("cargo:rustc-link-lib=rdmacm");
}
