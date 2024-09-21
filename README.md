# cuckooget
## What
A very fast website copy script using a cuckoo hash table. There are still many problems.
I feel sad about disappearing websites, and I’m thinking of ways to save them even faster.  
  
The cache file is saved as `hash_cache.json`, which prevents duplicates and ensures stable operation.  
This means that even if the process is interrupted, the existence of `hash_cache.json` allows for continuous saving of the same URL. This is possible because the hash table can be reloaded from `hash_cache.json`, enabling the continuation of the processing.  
`async_web_mirror.py` script manages the site copy, while hash management is handled by `cuckoo_hash.py`.

*Websites are our memories.*  
Let everyone rise up and preserve disappearing historical websites, leaving them for the future.  
For all geeks and for those who love the internet. If you find an interesting website, please contact me.  
  
Furthermore, with the `-w` option, you can set higher priorities based on the URL. I don't think other website mirroring software has this feature.
  
Collisions are avoided by the cuckoo hash table and generated by the ultra-fast xxhash.
It consists of xxh32 and xxh64 as different hash values.  
  
But I'm sorry. I can't give up on md5 because I like it, so I can't abandon it. I'll import `hashlib` since I'll be using the md5 hash for generating file names in `get_file_path`.
  
## Install
deps
```
pip install -r requirements.txt
chmod +x main.py
```

## Usage
```
usage: main.py [-h] [-c CONNECTIONS] [-w WEIGHTS [WEIGHTS ...]] [-v EXCLUDE [EXCLUDE ...]] url output_dir
```
