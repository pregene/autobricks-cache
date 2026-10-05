use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::Command;

fn run(command: &mut Command, name: &str) -> io::Result<()> {
    let status = command.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!("{name} failed with {status}")))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let version = fs::read_to_string("VERSION")?.trim().to_owned();
    if version != env::var("CARGO_PKG_VERSION")? {
        return Err(io::Error::other("VERSION and Cargo.toml package version do not match").into());
    }
    let output = PathBuf::from(
        env::var_os("OUT_DIR").ok_or_else(|| io::Error::other("OUT_DIR is missing"))?,
    );
    let product_metadata = format!("Autobricks Cache\0{version}\0(C) 2026 Autobricks, Co.\0");
    fs::write(
        output.join("product_metadata.rs"),
        format!(
            "#[no_mangle]\npub static AUTOBRICKS_CACHE_BINARY_METADATA: [u8; {}] = {:?};\n",
            product_metadata.len(),
            product_metadata.as_bytes()
        ),
    )?;
    let object = output.join("cache_core.o");
    let archive = output.join("libautobricks_cache_core.a");
    let compiler = env::var("CXX").unwrap_or_else(|_| "c++".to_owned());
    let target_os = env::var("CARGO_CFG_TARGET_OS")?;
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH")?;

    let mut compile = Command::new(&compiler);
    compile.args([
        "-std=c++20",
        "-O3",
        "-DNDEBUG",
        "-fPIC",
        "-Wall",
        "-Wextra",
        "-Werror",
        "-c",
    ]);
    if target_os == "macos" {
        let architecture = match target_arch.as_str() {
            "aarch64" => "arm64",
            "x86_64" => "x86_64",
            _ => return Err(io::Error::other("unsupported macOS architecture").into()),
        };
        compile.args(["-arch", architecture]);
    }
    compile
        .arg("src/core/cache_core.cpp")
        .arg("-o")
        .arg(&object);

    run(&mut compile, "C++ Cache Core compilation")?;
    run(
        Command::new("ar").arg("crs").arg(&archive).arg(&object),
        "C++ Cache Core archive",
    )?;

    println!("cargo:rerun-if-changed=src/core/cache_core.cpp");
    println!("cargo:rerun-if-changed=src/core/cache_core.h");
    println!("cargo:rerun-if-changed=VERSION");
    println!("cargo:rustc-env=AUTOBRICKS_CACHE_VERSION={version}");
    println!("cargo:rustc-link-search=native={}", output.display());
    println!("cargo:rustc-link-lib=static=autobricks_cache_core");
    if target_os == "macos" {
        println!("cargo:rustc-link-lib=dylib=c++");
        println!("cargo:rustc-cdylib-link-arg=-Wl,-install_name,@rpath/libautobricks_cache.dylib");
    } else {
        println!("cargo:rustc-link-lib=dylib=stdc++");
    }
    Ok(())
}
