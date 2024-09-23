use pyo3::prelude::*;

mod cuckoo_hash;
mod dag;

#[pymodule]
fn cuckoo_nest(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<cuckoo_hash::CuckooHash>()?;
    m.add_class::<dag::DAG>()?;
    Ok(())
}
