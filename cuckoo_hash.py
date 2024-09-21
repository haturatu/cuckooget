import os
import json
import random
import xxhash

class CuckooHash:
    def __init__(self, size, cache_file='hash_cache.json'):
        self.size = size
        self.table = [None] * size
        self.max_kicks = 500
        self.load_factor = 0
        self.resize_threshold = 0.75
        self.cache_file = cache_file

        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)
            print(f"Removed existing cache file: {self.cache_file}")

        self.hash_cache = {}

    def _save_cache(self):
        with open(self.cache_file, 'w') as f:
            json.dump(self.hash_cache, f)

    def hash1(self, key):
        return xxhash.xxh32(key).intdigest() % self.size

    def hash2(self, key):
        return xxhash.xxh64(key).intdigest() % self.size

    def insert(self, key, value):
        if self.get(key) is not None:
            return True

        if self.load_factor >= self.resize_threshold:
            self._resize()

        original_key, original_value = key, value
        for _ in range(self.max_kicks):
            key, value = self._insert(key, value)
            if key is None:
                self.load_factor = sum(1 for item in self.table if item is not None) / self.size
                self.hash_cache[original_key] = original_value
                self._save_cache()
                return True

            if (key, value) == (original_key, original_value):
                break

        self._resize()
        return self.insert(original_key, original_value)

    def _insert(self, key, value):
        h1, h2 = self.hash1(key), self.hash2(key)

        if self.table[h1] is None:
            self.table[h1] = (key, value)
            return None, None

        if self.table[h2] is None:
            self.table[h2] = (key, value)
            return None, None

        if random.choice([True, False]):
            self.table[h1], (key, value) = (key, value), self.table[h1]
        else:
            self.table[h2], (key, value) = (key, value), self.table[h2]

        return key, value

    def get(self, key):
        if key in self.hash_cache:
            return self.hash_cache[key]

        h1, h2 = self.hash1(key), self.hash2(key)

        if self.table[h1] and self.table[h1][0] == key:
            return self.table[h1][1]

        if self.table[h2] and self.table[h2][0] == key:
            return self.table[h2][1]

        return None

    def remove(self, key):
        h1, h2 = self.hash1(key), self.hash2(key)

        if self.table[h1] and self.table[h1][0] == key:
            self.table[h1] = None
            self.load_factor -= 1 / self.size
            if key in self.hash_cache:
                del self.hash_cache[key]
                self._save_cache()
        elif self.table[h2] and self.table[h2][0] == key:
            self.table[h2] = None
            self.load_factor -= 1 / self.size
            if key in self.hash_cache:
                del self.hash_cache[key]
                self._save_cache()

    def _resize(self):
        old_table = [item for item in self.table if item is not None]
        new_size = max(2 * len(old_table), 4)
        new_table = CuckooHash(new_size, self.cache_file)
        new_table.hash_cache = self.hash_cache.copy()
        for item in old_table:
            new_table.insert(item[0], item[1])
        self.size = new_table.size
        self.table = new_table.table
        self.hash_cache = new_table.hash_cache
