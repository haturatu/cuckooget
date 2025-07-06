use pyo3::prelude::*;
use pyo3::PyTypeInfo;

mod cuckoo_hash;
mod dag;

#[pymodule]
fn cuckoo_nest(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("CuckooHash", cuckoo_hash::CuckooHash::type_object(py))?;
    m.add("DAG", dag::DAG::type_object(py))?;
    Ok(())
}

