use std::collections::{HashMap, HashSet, VecDeque};
use pyo3::prelude::*;

#[pyclass]
pub struct DAG {
    graph: Vec<HashSet<usize>>,
    node_to_index: HashMap<String, usize>,
    index_to_node: Vec<String>,
    in_progress: HashSet<usize>,
}

#[pymethods]
impl DAG {
    #[new]
    pub fn new() -> Self {
        DAG {
            graph: Vec::new(),
            node_to_index: HashMap::new(),
            index_to_node: Vec::new(),
            in_progress: HashSet::new(),
        }
    }

    pub fn add_node(&mut self, node: String) -> PyResult<bool> {
        if let Some(&index) = self.node_to_index.get(&node) {
            if self.in_progress.contains(&index) {
                return Ok(false);
            }
            self.in_progress.insert(index);
        } else {
            let index = self.graph.len();
            self.graph.push(HashSet::new());
            self.node_to_index.insert(node.clone(), index);
            self.index_to_node.push(node);
            self.in_progress.insert(index);
        }
        Ok(true)
    }

    pub fn remove_node(&mut self, node: &str) -> PyResult<()> {
        if let Some(&index) = self.node_to_index.get(node) {
            self.in_progress.remove(&index);
        }
        Ok(())
    }

    pub fn add_edge(&mut self, from_node: &str, to_node: &str) -> PyResult<bool> {
        let from_index = self.get_or_create_index(from_node);
        let to_index = self.get_or_create_index(to_node);

        if self.graph[from_index].contains(&to_index) {
            return Ok(false);
        }

        if self.would_create_cycle(from_index, to_index) {
            return Ok(false);
        }

        self.graph[from_index].insert(to_index);
        Ok(true)
    }

    pub fn has_path(&self, start: &str, end: &str) -> PyResult<bool> {
        if let (Some(&start_index), Some(&end_index)) = (self.node_to_index.get(start), self.node_to_index.get(end)) {
            let mut visited = vec![false; self.graph.len()];
            let mut queue = VecDeque::new();
            queue.push_back(start_index);

            while let Some(node) = queue.pop_front() {
                if node == end_index {
                    return Ok(true);
                }
                if !visited[node] {
                    visited[node] = true;
                    queue.extend(self.graph[node].iter().filter(|&&n| !visited[n]));
                }
            }
        }
        Ok(false)
    }

    fn get_or_create_index(&mut self, node: &str) -> usize {
        if let Some(&index) = self.node_to_index.get(node) {
            index
        } else {
            let index = self.graph.len();
            self.graph.push(HashSet::new());
            self.node_to_index.insert(node.to_string(), index);
            self.index_to_node.push(node.to_string());
            index
        }
    }

    fn would_create_cycle(&self, from: usize, to: usize) -> bool {
        let mut visited = vec![false; self.graph.len()];
        let mut stack = vec![to];

        while let Some(node) = stack.pop() {
            if node == from {
                return true;
            }
            if !visited[node] {
                visited[node] = true;
                stack.extend(self.graph[node].iter().filter(|&&n| !visited[n]));
            }
        }

        false
    }
}
