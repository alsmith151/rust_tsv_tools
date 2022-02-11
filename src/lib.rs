use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
// use pyo3::types::{PyDict, IntoPyDict};
pub mod split_tabix;

// Splits a tabix files into separate files using supplied barcodes
#[pyfunction]
#[pyo3(name = "subsample_bam")]
fn split_tabix_py(
    tabix: String,
    barcodes: HashMap<String, HashSet<String>>,
    _n_threads: Option<u8>,
)  -> PyResult<()> 
{   

    ctrlc::set_handler(|| std::process::exit(2)).unwrap_or_default();
    let stats = split_tabix::split_tabix_by_barcode(&tabix, &barcodes).unwrap();
    println!("{:?}", stats);

    Ok(())

}

#[pymodule]
fn rust_tsv_tools(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(split_tabix_py, m)?)?;
    Ok(())
}
