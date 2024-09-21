# cuckooget
## What
A very fast website copy script using a cuckoo hash table. There are still many problems.
I feel sad about disappearing websites, and I’m thinking of ways to save them even faster.  
  
The cache file is stored in `hash_cache.json`, which avoids duplication and results in very low memory usage, ensuring stable operation. It is very simple.  
`async_web_mirror.py` script manages the site copy, while hash management is handled by `cuckoo_hash.py`.

*Websites are our memories.*  
Let everyone rise up and preserve disappearing historical websites, leaving them for the future.  
For all geeks and for those who love the internet. If you find an interesting website, please contact me.  
  
Furthermore, with the `-w` option, you can set higher priorities based on the URL. I don't think other website mirroring software has this feature.
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
