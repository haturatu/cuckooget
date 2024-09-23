class DAG:
    def __init__(self):
        self.graph = {}
        self.in_progress = set()

    def add_node(self, node):
        if node in self.in_progress:
            return False
        self.in_progress.add(node)
        if node not in self.graph:
            self.graph[node] = set()
        return True

    def remove_node(self, node):
        self.in_progress.remove(node)

    def add_edge(self, from_node, to_node):
        if from_node not in self.graph:
            self.graph[from_node] = set()
        if to_node not in self.graph:
            self.graph[to_node] = set()

        if to_node in self.graph[from_node]:
            return False

        if self._has_path(to_node, from_node):
            return False

        self.graph[from_node].add(to_node)
        return True

    def _has_path(self, start, end):
        visited = set()
        stack = [start]

        while stack:
            node = stack.pop()
            if node == end:
                return True
            if node not in visited:
                visited.add(node)
                stack.extend(self.graph.get(node, []))

        return False
