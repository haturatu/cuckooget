use pyo3::prelude::*;
use rand::random;
use xxhash_rust::xxh32::Xxh32;
use xxhash_rust::xxh64::Xxh64;

#[pyclass]
struct CuckooHash {
    table1: Vec<Option<(String, String)>>,
    table2: Vec<Option<(String, String)>>,
    size: usize,
    item_count: usize,
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
            item_count: 0,
            max_kicks: 500,
            resize_threshold: 0.5, // 50%でリサイズ
        }
    }

    // ハッシュ関数1
    fn hash1(&self, key: &str) -> usize {
        let mut hasher = Xxh32::new(0);
        hasher.update(key.as_bytes());
        (hasher.digest() as usize) % self.size
    }

    // ハッシュ関数2
    fn hash2(&self, key: &str) -> usize {
        let mut hasher = Xxh64::new(0);
        hasher.update(key.as_bytes());
        (hasher.digest() as usize) % self.size
    }

    // 要素を挿入
    pub fn insert(&mut self, key: String, value: String) -> bool {
        if self.item_count as f64 / (2 * self.size) as f64 >= self.resize_threshold {
            self.resize();
        }

        for _ in 0..self.max_kicks {
            // ハッシュテーブル1への挿入を試みる
            let h1 = self.hash1(&key);
            if self.table1[h1].is_none() {
                self.table1[h1] = Some((key.clone(), value.clone()));
                self.item_count += 1;
                return true;
            }

            // ハッシュテーブル2への挿入を試みる
            let h2 = self.hash2(&key);
            if self.table2[h2].is_none() {
                self.table2[h2] = Some((key.clone(), value.clone()));
                self.item_count += 1;
                return true;
            }

            // ランダムにスワップを行う
            if random::<bool>() {
                std::mem::swap(&mut self.table1[h1], &mut Some((key.clone(), value.clone())));
            } else {
                std::mem::swap(&mut self.table2[h2], &mut Some((key.clone(), value.clone())));
            }
        }

        // 最大スワップ回数に達した場合は失敗
        false
    }

    // 要素を取得
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

    // 要素を削除
    pub fn remove(&mut self, key: &str) -> bool {
        let h1 = self.hash1(key);
        if let Some((ref k, _)) = self.table1[h1] {
            if k == key {
                self.table1[h1] = None;
                self.item_count -= 1;
                return true;
            }
        }

        let h2 = self.hash2(key);
        if let Some((ref k, _)) = self.table2[h2] {
            if k == key {
                self.table2[h2] = None;
                self.item_count -= 1;
                return true;
            }
        }

        false
    }

    // テーブルをリサイズ
    fn resize(&mut self) {
        let new_size = self.size * 2;
        let mut new_table1 = vec![None; new_size];
        let mut new_table2 = vec![None; new_size];

        // ハッシュテーブルの要素を新しいテーブルに再配置
        for entry in self.table1.iter().chain(self.table2.iter()) {
            if let Some((key, value)) = entry {
                let h1 = self.hash1(key) % new_size;
                if new_table1[h1].is_none() {
                    new_table1[h1] = Some((key.clone(), value.clone()));
                } else {
                    let h2 = self.hash2(key) % new_size;
                    new_table2[h2] = Some((key.clone(), value.clone()));
                }
            }
        }

        self.table1 = new_table1;
        self.table2 = new_table2;
        self.size = new_size;
    }
}

#[pymodule]
fn cuckoo_hash(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<CuckooHash>()?;
    Ok(())
}

