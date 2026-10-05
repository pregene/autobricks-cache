use std::ffi::CStr;
use std::os::raw::c_char;
use std::path::Path;

#[no_mangle]
pub unsafe extern "C" fn ab_cache_run_benchmark(project_directory: *const c_char) -> i32 {
    let outcome = std::panic::catch_unwind(|| {
        if project_directory.is_null() {
            return Err("project_directory is null".to_owned());
        }
        let directory = CStr::from_ptr(project_directory)
            .to_str()
            .map_err(|error| format!("project_directory is not UTF-8: {error}"))?;
        let root = Path::new(directory);
        if std::env::var_os("AB_CACHE_PURPOSE_TEST").is_some() {
            crate::benchmark::run_purpose(root).map_err(|error| error.to_string())
        } else {
            crate::benchmark::run(root).map_err(|error| error.to_string())
        }
    });
    match outcome {
        Ok(Ok(())) => 0,
        Ok(Err(error)) => {
            eprintln!("benchmark failed: {error}");
            1
        }
        Err(_) => {
            eprintln!("benchmark failed: shared library panicked");
            2
        }
    }
}
