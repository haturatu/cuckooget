use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use xxhash_rust::xxh32::Xxh32;
use xxhash_rust::xxh64::Xxh64;

#[pyclass]
struct CuckooHash {
    table1: Vec<Option<(String, String)>>,
    table2: Vec<Option<(String, String)>>,
    size: usize,
    load_factor: f64,
    max_kicks: usize,
    resize_threshold: f64,
}

#[pymethods]
impl CuckooHash {
    #[new]
    fn new(size: usize) -> Self {
        CuckooHash {
            table1: vec![None; size],
            table2: vec![None; size],
            size,
            load_factor: 0.0,
            max_kicks: 500,
            resize_threshold: 0.5,
        }
    }

    fn hash1(&self, key: &str) -> usize {
        let mut hasher = Xxh32::new(0);
        hasher.update(key.as_bytes());
        (hasher.digest() as usize) % self.size
    }

    fn hash2(&self, key: &str) -> usize {
        let mut hasher = Xxh64::new(0);
        hasher.update(key.as_bytes());
        (hasher.digest() as usize) % self.size
    }
    pub fn insert(&mut self, key: String, value: String) -> bool {
        if self.load_factor >= self.resize_threshold {
            self.resize();
        }

        let mut key = key;
        let value = value;

        for _ in 0..self.max_kicks {
            let h1 = self.hash1(&key);
            if self.table1[h1].is_none() {
                self.table1[h1] = Some((key.clone(), value.clone())); // keyとvalueをクローン
                self.load_factor += 1.0 / (2 * self.size) as f64;
                return true;
            }

            let h2 = self.hash2(&key);
            if self.table2[h2].is_none() {
                self.table2[h2] = Some((key.clone(), value.clone())); // keyとvalueをクローン
                self.load_factor += 1.0 / (2 * self.size) as f64;
                return true;
            }

            if rand::random::<bool>() {
                std::mem::swap(
                    &mut self.table1[h1],
                    &mut Some((key.clone(), value.clone())),
                ); // keyとvalueをクローン
            } else {
                std::mem::swap(
                    &mut self.table2[h2],
                    &mut Some((key.clone(), value.clone())),
                ); // keyとvalueをクローン
            }
        }

        false
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let h1 = self.hash1(key);
        if let Some((ref k, ref v)) = self.table1[h1] {
            if k == key {
                return Some(v.clone());
            }
        }

        let h2 = self.hash2(key);
        if let Some((ref k, ref v)) = self.table2[h2] {
            if k == key {
                return Some(v.clone());
            }
        }

        None
    }

    pub fn remove(&mut self, key: &str) -> bool {
        let h1 = self.hash1(key);
        if let Some((ref k, _)) = self.table1[h1] {
            if k == key {
                self.table1[h1] = None;
                self.load_factor -= 1.0 / (2 * self.size) as f64;
                return true;
            }
        }

        let h2 = self.hash2(key);
        if let Some((ref k, _)) = self.table2[h2] {
            if k == key {
                self.table2[h2] = None;
                self.load_factor -= 1.0 / (2 * self.size) as f64;
                return true;
            }
        }

        false
    }

    fn resize(&mut self) {
        let old_table1 = std::mem::replace(&mut self.table1, vec![None; self.size * 2]);
        let old_table2 = std::mem::replace(&mut self.table2, vec![None; self.size * 2]);
        self.size *= 2;
        self.load_factor = 0.0;

        for table in &[old_table1, old_table2] {
            for entry in table.iter() {
                if let Some((key, value)) = entry {
                    self.insert(key.clone(), value.clone());
                }
            }
        }
    }
}

#[pymodule]
fn cuckoo_hash(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<CuckooHash>()?;
    Ok(())
}
