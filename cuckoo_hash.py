import xxhash

class CuckooHash:
    def __init__(self, size):
        self.size = size
        self.table1 = [None] * size
        self.table2 = [None] * size
        self.max_kicks = 500
        self.load_factor = 0
        self.resize_threshold = 0.5

    def hash1(self, key):
        return xxhash.xxh32(key.encode()).intdigest() % self.size

    def hash2(self, key):
        return xxhash.xxh64(key.encode()).intdigest() % self.size

    def insert(self, key, value):
        if self.load_factor >= self.resize_threshold:
            self._resize()

        for _ in range(self.max_kicks):
            h1, h2 = self.hash1(key), self.hash2(key)
            if self.table1[h1] is None:
                self.table1[h1] = (key, value)
                self.load_factor += 1 / (2 * self.size)
                return True
            if self.table2[h2] is None:
                self.table2[h2] = (key, value)
                self.load_factor += 1 / (2 * self.size)
                return True
            if xxhash.xxh32(key.encode()).intdigest() % 2 == 0:
                self.table1[h1], (key, value) = (key, value), self.table1[h1]
            else:
                self.table2[h2], (key, value) = (key, value), self.table2[h2]
        return False

    def get(self, key):
        h1, h2 = self.hash1(key), self.hash2(key)
        if self.table1[h1] and self.table1[h1][0] == key:
            return self.table1[h1][1]
        if self.table2[h2] and self.table2[h2][0] == key:
            return self.table2[h2][1]
        return None

    def remove(self, key):
        h1, h2 = self.hash1(key), self.hash2(key)
        if self.table1[h1] and self.table1[h1][0] == key:
            self.table1[h1] = None
            self.load_factor -= 1 / (2 * self.size)
            return True
        if self.table2[h2] and self.table2[h2][0] == key:
            self.table2[h2] = None
            self.load_factor -= 1 / (2 * self.size)
            return True
        return False

    def _resize(self):
        old_table1, old_table2 = self.table1, self.table2
        self.size *= 2
        self.table1 = [None] * self.size
        self.table2 = [None] * self.size
        self.load_factor = 0
        for table in (old_table1, old_table2):
            for item in table:
                if item:
                    self.insert(item[0], item[1])
