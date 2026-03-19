use std::collections::{HashMap, HashSet, VecDeque};
use pyo3::prelude::*;

#[pyclass]
pub struct DAG {
    graph: Vec<Option<HashSet<usize>>>,
    node_to_index: HashMap<String, usize>,
    index_to_node: Vec<Option<String>>,
    in_progress: HashSet<usize>,
    free_indices: Vec<usize>,
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
            free_indices: Vec::new(),
        }
    }

    pub fn add_node(&mut self, node: String) -> PyResult<bool> {
        if let Some(&index) = self.node_to_index.get(&node) {
            if self.in_progress.contains(&index) {
                return Ok(false);
            }
            self.in_progress.insert(index);
        } else {
            let index = self.allocate_index(node.clone());
            self.node_to_index.insert(node.clone(), index);
            self.in_progress.insert(index);
        }
        Ok(true)
    }

    pub fn finish_node(&mut self, node: &str) -> PyResult<()> {
        if let Some(index) = self.node_to_index.remove(node) {
            self.in_progress.remove(&index);
            self.release_index(index);
        }
        Ok(())
    }

    pub fn remove_node(&mut self, node: &str) -> PyResult<()> {
        self.finish_node(node)
    }

    pub fn add_edge(&mut self, from_node: &str, to_node: &str) -> PyResult<bool> {
        if from_node == to_node {
            return Ok(false);
        }

        let from_index = self.node_to_index.get(from_node).copied();
        let to_index = self.node_to_index.get(to_node).copied();

        if let (Some(from_index), Some(to_index)) = (from_index, to_index) {
            if self.graph[from_index]
                .as_ref()
                .expect("graph slot must exist for active node")
                .contains(&to_index)
            {
                return Ok(false);
            }

            if self.would_create_cycle(from_index, to_index) {
                return Ok(false);
            }
        }

        let from_index = self.get_or_create_index(from_node);
        let to_index = self.get_or_create_index(to_node);

        self.graph[from_index]
            .as_mut()
            .expect("graph slot must exist for active node")
            .insert(to_index);
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
                    if let Some(neighbors) = &self.graph[node] {
                        queue.extend(neighbors.iter().copied().filter(|&n| !visited[n]));
                    }
                }
            }
        }
        Ok(false)
    }

    fn get_or_create_index(&mut self, node: &str) -> usize {
        if let Some(&index) = self.node_to_index.get(node) {
            index
        } else {
            let index = self.allocate_index(node.to_string());
            self.node_to_index.insert(node.to_string(), index);
            index
        }
    }

    fn allocate_index(&mut self, node: String) -> usize {
        if let Some(index) = self.free_indices.pop() {
            self.graph[index] = Some(HashSet::new());
            self.index_to_node[index] = Some(node);
            index
        } else {
            let index = self.graph.len();
            self.graph.push(Some(HashSet::new()));
            self.index_to_node.push(Some(node));
            index
        }
    }

    fn release_index(&mut self, index: usize) {
        self.graph[index] = None;
        self.index_to_node[index] = None;

        for edges in self.graph.iter_mut().flatten() {
            edges.remove(&index);
        }

        self.free_indices.push(index);
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
                if let Some(neighbors) = &self.graph[node] {
                    stack.extend(neighbors.iter().copied().filter(|&n| !visited[n]));
                }
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::DAG;

    #[test]
    fn add_node_blocks_only_while_in_progress() {
        let mut dag = DAG::new();

        assert!(dag.add_node("a".to_string()).unwrap());
        assert!(!dag.add_node("a".to_string()).unwrap());

        dag.finish_node("a").unwrap();

        assert!(dag.add_node("a".to_string()).unwrap());
    }

    #[test]
    fn add_edge_rejects_self_cycle_without_creating_node() {
        let mut dag = DAG::new();

        assert!(!dag.add_edge("a", "a").unwrap());
        assert!(!dag.has_path("a", "a").unwrap());
    }

    #[test]
    fn add_edge_cycle_failure_does_not_mutate_graph() {
        let mut dag = DAG::new();

        assert!(dag.add_edge("a", "b").unwrap());
        assert!(dag.add_edge("b", "c").unwrap());
        assert!(!dag.add_edge("c", "a").unwrap());

        assert!(dag.has_path("a", "c").unwrap());
        assert!(!dag.has_path("c", "a").unwrap());
    }

    #[test]
    fn add_edge_allows_missing_nodes_when_no_cycle_is_possible() {
        let mut dag = DAG::new();

        assert!(dag.add_edge("a", "b").unwrap());
        assert!(dag.has_path("a", "b").unwrap());
    }

    #[test]
    fn finish_node_removes_node_and_edges() {
        let mut dag = DAG::new();

        assert!(dag.add_edge("a", "b").unwrap());
        dag.finish_node("a").unwrap();

        assert!(!dag.has_path("a", "b").unwrap());
        assert_eq!(dag.node_to_index.len(), 1);
    }

    #[test]
    fn finish_node_reuses_freed_index() {
        let mut dag = DAG::new();

        assert!(dag.add_node("a".to_string()).unwrap());
        let a_index = dag.node_to_index["a"];

        dag.finish_node("a").unwrap();
        assert!(dag.add_node("b".to_string()).unwrap());

        assert_eq!(dag.node_to_index["b"], a_index);
        assert_eq!(dag.graph.len(), 1);
    }
}
