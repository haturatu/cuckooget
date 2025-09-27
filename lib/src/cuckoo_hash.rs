use pyo3::prelude::*;
use rand::random;
use xxhash_rust::xxh32::Xxh32;
use xxhash_rust::xxh64::Xxh64;

#[pyclass]
pub struct CuckooHash {
    table1: Vec<Option<(String, String)>>,
    table2: Vec<Option<(String, String)>>,
    size: usize,
    original_size: usize,
    resize_multiplier: usize,
    item_count: usize,
    max_kicks: usize,
}

impl CuckooHash {
    fn insert_internal(&mut self, mut key: String, mut value: String) -> Result<(), (String, String)> {
        for _ in 0..self.max_kicks {
            let h1 = self.hash1(&key);
            if self.table1[h1].is_none() {
                self.table1[h1] = Some((key, value));
                self.item_count += 1;
                return Ok(());
            }

            let h2 = self.hash2(&key);
            if self.table2[h2].is_none() {
                self.table2[h2] = Some((key, value));
                self.item_count += 1;
                return Ok(());
            }

            // Both slots are full, time to kick one out.
            if random::<bool>() {
                let swapped_out = std::mem::replace(
                    &mut self.table1[h1],
                    Some((key, value))
                ).unwrap();
                key = swapped_out.0;
                value = swapped_out.1;
            } else {
                let swapped_out = std::mem::replace(
                    &mut self.table2[h2],
                    Some((key, value))
                ).unwrap();
                key = swapped_out.0;
                value = swapped_out.1;
            }
        }
        Err((key, value))
    }

    // テーブルをリサイズ
    fn resize(&mut self) {
        let new_size = self.original_size * self.resize_multiplier;
        self.resize_multiplier += 1;
        
        let old_items: Vec<(String, String)> = self.table1.drain(..).chain(self.table2.drain(..))
            .filter_map(|opt| opt)
            .collect();

        self.table1 = vec![None; new_size];
        self.table2 = vec![None; new_size];
        self.size = new_size;
        self.item_count = 0;

        for (key, value) in old_items {
            if self.insert_internal(key, value).is_err() {
                // ToDo: リサイズ後の挿入に失敗した場合の処理
            }
        }
    }
}

#[pymethods]
impl CuckooHash {
    #[new]
    pub fn new(size: usize) -> Self {
        CuckooHash {
            table1: vec![None; size],
            table2: vec![None; size],
            size,
            original_size: size,
            resize_multiplier: 2,
            item_count: 0,
            max_kicks: 500,
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
        if let Err((k, v)) = self.insert_internal(key, value) {
            self.resize();
            if self.insert_internal(k, v).is_err() {
                // ToDo: 
                // 再リサイズ後も失敗した場合
                // ここでさらにリサイズを試みるか、エラーを返す
                return false;
            }
        }
        true
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
}
