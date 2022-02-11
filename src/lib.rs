use std::collections::{HashMap, HashSet};

use pyo3::prelude::*;
// use pyo3::types::{PyDict, IntoPyDict};
pub mod split_tabix;

// Splits a tabix files into separate files using supplied barcodes
#[pyfunction]
#[pyo3(text_signature = "(tabix, barcodes, /)")]
#[pyo3(name = "split_tabix_by_barcode")]
fn split_tabix_by_barcode_py(
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

// Splits a tabix files into separate files by fragment length
#[pyfunction]
#[pyo3(text_signature = "(tabix, bins, /)")]
#[pyo3(name = "split_tabix_by_barcode")]
fn split_tabix_by_fragemnt_length_py(
    tabix: String,
    bins: HashMap<String, Vec<u64>>,
    _n_threads: Option<u8>,
)  -> PyResult<()> 
{   

    ctrlc::set_handler(|| std::process::exit(2)).unwrap_or_default();
    let stats = split_tabix::split_tabix_by_fragment_size(&tabix, &bins).unwrap();
    println!("{:?}", stats);

    Ok(())

}

#[pymodule]
fn rust_tsv_tools(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(split_tabix_by_barcode_py, m)?)?;
    m.add_function(wrap_pyfunction!(split_tabix_by_fragemnt_length_py, m)?)?;
    Ok(())
}
