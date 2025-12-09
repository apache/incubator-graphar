use std::path::{Path, PathBuf};

fn link_libraries() {
    // build from source should enable this
    println!("cargo:rustc-link-search=native=/usr/lib/x86_64-linux-gnu/");
    println!("cargo:rustc-link-lib=dylib=arrow_compute");
    println!("cargo:rustc-link-lib=dylib=arrow_dataset");
    println!("cargo:rustc-link-lib=dylib=arrow_acero");
    println!("cargo:rustc-link-lib=dylib=arrow");

    println!("cargo:rustc-link-lib=graphar");
}

fn build_ffi(bridge_file: &str, out_name: &str, source_file: &str, include_paths: &Vec<PathBuf>) {
    let mut build = cxx_build::bridge(bridge_file);
    build.file(source_file);

    build.includes(include_paths);
    build.flag("-std=c++17");
    build.flag("-fdiagnostics-color=always");

    build.compile(out_name);
}

fn build_graphar() -> Vec<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("cpp");
    let mut build = cmake::Config::new(&root);
    build
        .no_build_target(true)
        .define("CMAKE_BUILD_TYPE", "RelWithDebInfo")
        .define("GRAPHAR_BUILD_STATIC", "on");
    let build_dir = build.build();

    let lib_path = build_dir.join("build");
    println!("cargo:rustc-link-search=native={}", lib_path.display());

    println!("cargo:rerun-if-changed=include/graphar_rs.h");
    println!("cargo:rerun-if-changed=src/graphar_rs.cc");
    println!("cargo:rerun-if-changed=../cpp");

    vec![root.join("src/"), root.join("thirdparty/")]
}

fn main() {
    let mut include_paths = vec![Path::new(env!("CARGO_MANIFEST_DIR")).join("include")];
    include_paths.extend(build_graphar());

    link_libraries();

    build_ffi(
        "src/ffi.rs",
        "graphar_cxx",
        "src/graphar_rs.cc",
        &include_paths,
    );
}
