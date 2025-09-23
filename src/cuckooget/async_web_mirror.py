import asyncio
import aiohttp
from aiohttp import ClientSession
import aiofiles
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import xxhash
from cuckoo_nest import CuckooHash, DAG
import ujson
import json
import time

class URLState:
    """Manages the state of URLs: visited, completed, and path mappings."""
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.lock = asyncio.Lock()

        # --- State data structures ---
        self.visited = CuckooHash(100000)
        self.path_map = CuckooHash(100000)
        self.completed_urls = set()
        self.url_to_path = {}

        # --- State file paths ---
        self.state_file = os.path.join(self.output_dir, "state.json")
        self.temp_state_file = os.path.join(self.output_dir, "state.json.tmp")
        self.completion_flag_file = os.path.join(self.output_dir, "COMPLETE")

        # Load previous state on initialization
        self.load_state()

    def load_state(self):
        """Load previous download state from file."""
        # Check if the download was completed previously
        if os.path.exists(self.completion_flag_file):
            print("Previous download was completed successfully.")
            return
            
        if not os.path.exists(self.state_file):
            print("No previous state found. Starting fresh download.")
            return
            
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state_data = json.load(f)
                
            # Load URL mappings
            self.url_to_path = state_data.get('url_to_path', {})
            # Load set of completed URLs
            self.completed_urls = set(state_data.get('completed_urls', []))
            
            # Update CuckooHash structures from the loaded state
            for url, path in self.url_to_path.items():
                self.visited.insert(url, "True")
                self.path_map.insert(url, path)
                
            print(f"Loaded previous state: {len(self.url_to_path)} visited URLs, {len(self.completed_urls)} completed.")
        except Exception as e:
            # If state is corrupted, start fresh
            print(f"Error loading previous state: {e}. Starting fresh.")
            self.url_to_path = {}
            self.completed_urls = set()

    async def save_state(self, force=False, processed_count=0, last_save_time=0):
        """Save current download state to a file atomically."""
        current_time = time.time()
        
        # Save state only if forced, or at specific intervals to avoid performance issues
        if (not force and 
            processed_count % 5 != 0 and
            current_time - last_save_time < 30):
            return False, last_save_time

        async with self.lock:
            try:
                state_data = {
                    'url_to_path': self.url_to_path,
                    # Visited URLs are derived from the keys of url_to_path to reduce data duplication
                    'visited_urls': list(self.url_to_path.keys()),
                    'completed_urls': list(self.completed_urls)
                }
                
                # First write to a temporary file
                with open(self.temp_state_file, 'w', encoding='utf-8') as f:
                    json.dump(state_data, f)
                    
                # Then rename to the actual state file (atomic operation)
                os.replace(self.temp_state_file, self.state_file)
                
                if force:
                    print(f"State saved: {len(self.url_to_path)} visited, {len(self.completed_urls)} completed.")
                return True, current_time
            except Exception as e:
                print(f"Error saving state: {e}")
                return False, last_save_time

    async def mark_download_complete(self):
        """Mark the download as completely finished."""
        # The final state is saved by the finally block in mirror_site.
        # This method is only responsible for creating the completion flag.

        # Create completion flag file
        with open(self.completion_flag_file, 'w', encoding='utf-8') as f:
            f.write(f"Download completed at {time.ctime()}\n")
            f.write(f"Total URLs processed: {len(self.completed_urls)}")
            
        print(f"Download marked as complete. Total URLs: {len(self.completed_urls)}")

    def is_download_completed(self):
        return os.path.exists(self.completion_flag_file)

    def get_pending_urls(self):
        """Identify URLs that were visited but not completed."""
        return set(self.url_to_path.keys()) - self.completed_urls

    async def is_visited(self, url):
        async with self.lock:
            return self.visited.get(url) is not None

    async def add_visited(self, url):
        """Atomically checks if a URL is visited, and if not, marks it as visited.
        Returns True if the URL was newly added, False otherwise."""
        async with self.lock:
            if self.visited.get(url) is None:
                self.visited.insert(url, "True")
                # Add to url_to_path immediately to mark as visited
                if url not in self.url_to_path:
                    self.url_to_path[url] = "" # Placeholder path until saved
                return True
            return False

    async def add_completed(self, url, path):
        """Atomically mark a URL as completed with its file path."""
        async with self.lock:
            self.path_map.insert(url, path)
            self.url_to_path[url] = path
            self.completed_urls.add(url)

    async def get_path(self, url):
        async with self.lock:
            return self.url_to_path.get(url)

class AsyncWebMirror:
    def __init__(self, start_url, output_dir, max_connections=50, weights=None, excluded_urls=None):
        self.start_url = start_url
        self.output_dir = output_dir
        self.domain = urlparse(start_url).netloc
        self.max_connections = max_connections
        self.weights = weights or []
        self.excluded_urls = excluded_urls or []
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # --- State and DAG Management ---
        self.state = URLState(self.output_dir)
        self.dag = DAG()
        self.dag_lock = asyncio.Lock() # Lock specifically for DAG operations

        # --- Concurrency Control ---
        self.semaphore = asyncio.Semaphore(max_connections)
        self.image_semaphore = asyncio.Semaphore(max_connections * 2)
        self.task_queue = asyncio.PriorityQueue()
        
        self.session = None
        self.processed_count = 0
        self.last_state_save_time = time.time()

    async def save_state_if_needed(self, force=False):
        """Wrapper to trigger state saving with current progress."""
        saved, new_time = await self.state.save_state(force, self.processed_count, self.last_state_save_time)
        if saved:
            self.last_state_save_time = new_time

    async def download_resource(self, url):
        async with self.dag_lock:
            if not self.dag.add_node(url):  # Prevent concurrent downloads for the same URL
                return None, None
        try:
            max_retries = 5
            retry_delay = 1
            for attempt in range(max_retries):
                try:
                    async with self.semaphore:
                        async with self.session.get(url, timeout=3) as response:
                            if response.status == 200:
                                content_type = response.headers.get('content-type', '').split(';')[0]
                                if content_type.startswith('text') or url.endswith(('.php', '.pl')):
                                    return await response.text(), 'text/html'
                                else:
                                    return await response.read(), content_type
                            elif 500 <= response.status < 600:
                                print(f"Retrying {url} due to server error {response.status} (attempt {attempt + 1})")
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2
                            else:
                                print(f"Error downloading {url}: Status {response.status}")
                                return None, None
                except (asyncio.TimeoutError, aiohttp.ClientPayloadError) as e:
                    print(f"Network error for {url}: {type(e).__name__} (attempt {attempt + 1})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                except Exception as e:
                    print(f"Unhandled error downloading {url}: {e}")
                    return None, None # Break on unknown exceptions
            
            # If all retries fail
            print(f"Failed to download {url} after {max_retries} attempts.")
            return None, None
        finally:
            # Ensure the DAG node is always removed once processing is complete
            async with self.dag_lock:
                self.dag.remove_node(url)

    def get_file_path(self, url):
        parsed_url = urlparse(url)
        path = parsed_url.path
        query = parsed_url.query

        if not path or path.endswith('/'):
            path = os.path.join(path, 'index.html')
        elif '.' not in os.path.basename(path) or path.endswith(('.php', '.pl')):
            path = f"{path}.html"

        if query:
            query_hash = xxhash.xxh32(query.encode()).hexdigest()[:8]
            base, ext = os.path.splitext(path)
            path = f"{base}_{query_hash}{ext}"

        # Handle filename length limits
        max_filename_length = 255  # Common filesystem limit
        if len(os.path.basename(path)) > max_filename_length:
            filename_hash = xxhash.xxh32(path.encode()).hexdigest()[:16]
            _, ext = os.path.splitext(path)
            path = f"{filename_hash}{ext}"

        return os.path.join(self.output_dir, parsed_url.netloc, path.lstrip('/'))

    async def save_resource(self, url, content, content_type):
        file_path = self.get_file_path(url)
        parent_dir = os.path.dirname(file_path)

        # Handle case where parent directory exists as a file
        if os.path.exists(parent_dir) and not os.path.isdir(parent_dir):
            new_name = f"{parent_dir}_{xxhash.xxh32(parent_dir.encode()).hexdigest()[:8]}"
            os.rename(parent_dir, new_name)
            print(f"Renamed existing file to: {new_name}")

        os.makedirs(parent_dir, exist_ok=True)

        mode = 'w' if 'text' in content_type else 'wb'
        encoding = 'utf-8' if 'text' in content_type else None

        # If file already exists, update state but don't rewrite
        if os.path.exists(file_path):
            print(f"File already exists: {file_path}")
            relative_path = os.path.relpath(file_path, self.output_dir)
            # Mark as completed
            await self.state.add_completed(url, relative_path)
            # Update counters and save state if needed
            self.processed_count += 1
            await self.save_state_if_needed()
            return relative_path

        try:
            async with aiofiles.open(file_path, mode=mode, encoding=encoding) as f:
                await f.write(content)
        except Exception as e:
            print(f"Error saving {url}: {e}")
            return None

        relative_path = os.path.relpath(file_path, self.output_dir)
        # Mark as completed
        await self.state.add_completed(url, relative_path)
        # Update counters and save state if needed
        self.processed_count += 1
        await self.save_state_if_needed()
        return relative_path

    async def process_links(self, soup, base_url):
        links = []
        for tag in soup.find_all(['a', 'link', 'script', 'img']):
            attr = 'href' if tag.name in ['a', 'link'] else 'src'
            url = tag.get(attr)
            if not url or url.startswith('#'):
                continue
            
            full_url = urljoin(base_url, url)
            is_visited = await self.state.is_visited(full_url)
            if (self.domain in full_url and 
                not is_visited and 
                not any(excluded in full_url for excluded in self.excluded_urls)):
                priority = self.get_url_priority(full_url)
                links.append((priority, full_url, (tag.name, attr)))
        return links

    def get_url_priority(self, url):
        for i, weight in enumerate(self.weights):
            if weight in url:
                return i  # Lower index = higher priority
        return len(self.weights)  # Default lowest priority

    async def get_relative_path(self, from_url, to_url):
        from_path = await self.state.get_path(from_url)
        to_path = await self.state.get_path(to_url)
        if from_path and to_path:
            # Ensure from_path is a file path for dirname to work correctly
            dir_path = os.path.dirname(from_path) if '.' in os.path.basename(from_path) else from_path
            return os.path.relpath(to_path, dir_path)
        return None

    async def mirror_site(self):
        # Check if download was previously completed
        if self.state.is_download_completed():
            print("Download was already completed. Use --force to download again.")
            return
            
        # Add pending URLs from previous run to the queue first
        pending_urls = self.state.get_pending_urls()
        if pending_urls:
            print(f"Resuming download of {len(pending_urls)} pending URLs")
            for url in pending_urls:
                await self.task_queue.put((self.get_url_priority(url), 0, url, None))

        # Add start URL if it hasn't been visited at all
        if not await self.state.is_visited(self.start_url):
            await self.task_queue.put((0, 0, self.start_url, None))

        # Create client session with optimal settings
        connector = aiohttp.TCPConnector(limit=self.max_connections, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=600)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout, json_serialize=ujson.dumps) as self.session:
            tasks = set()
            try:
                while not self.task_queue.empty() or tasks:
                    # Process URLs while maintaining concurrency limits
                    while len(tasks) < self.max_connections and not self.task_queue.empty():
                        _, count, url, tag_info = await self.task_queue.get()

                        # Avoid reprocessing already completed URLs
                        if url in self.state.completed_urls:
                            continue
                            
                        # Create a task for any URL that is in the queue and not completed.
                        # The logic to prevent duplication is now handled before items are added to the queue.
                        task = asyncio.create_task(self.process_url(url, tag_info, count))
                        tasks.add(task)

                    if not tasks: continue

                    # Wait for at least one task to complete
                    done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        try:
                            await task
                        except Exception as e:
                            print(f"Task error: {e}")
            except KeyboardInterrupt:
                print("\nDownload interrupted. Progress will be saved.")
            except Exception as e:
                print(f"\nError during mirroring: {e}")
            finally:
                # Ensure state is saved on exit
                await self.save_state_if_needed(force=True)

        # Mark download as complete if all tasks are finished
        if self.task_queue.empty() and not tasks:
            await self.state.mark_download_complete()

    async def process_url(self, url, tag_info, count):
        print(f"Downloading: {url}")
        content, content_type = await self.download_resource(url)
        if content is None:
            print(f"Failed to download: {url}")
            return

        if 'text/html' in content_type:
            soup = BeautifulSoup(content, 'html.parser')

            # Collect and process image tasks
            img_tasks = []
            for img_tag in soup.find_all('img', src=True):
                img_url = urljoin(url, img_tag['src'])
                if not await self.state.is_visited(img_url):
                    img_tasks.append(self.download_and_save_image(img_url, img_tag, url))

            # Process links, atomically mark as visited, and add to queue
            links = await self.process_links(soup, url)
            for priority, new_url, new_tag_info in links:
                # Atomically check and mark as visited. If it's newly visited, queue it.
                if await self.state.add_visited(new_url):
                    async with self.dag_lock:
                        self.dag.add_edge(url, new_url)
                    await self.task_queue.put((priority, count + 1, new_url, new_tag_info))
                    await self.save_state_if_needed()

            # Rewrite links to use relative paths
            for tag in soup.find_all(['a', 'link', 'img', 'script']):
                attr = 'href' if tag.name in ['a', 'link'] else 'src'
                if tag.get(attr):
                    full_url = urljoin(url, tag[attr])
                    relative_path = await self.get_relative_path(url, full_url)
                    if relative_path:
                        tag[attr] = relative_path
                    elif self.domain in full_url:
                        # Fallback for when path isn't in state yet
                        tag[attr] = os.path.relpath(self.get_file_path(full_url), os.path.dirname(self.get_file_path(url)))

            # Special handling for PHP/PL links
            for a_tag in soup.find_all('a', href=True):
                if a_tag['href'].endswith(('.php', '.pl')):
                    a_tag['href'] = a_tag['href'].rsplit('.', 1)[0] + '.html'

            # Wait for all image tasks to complete
            await asyncio.gather(*img_tasks)
            # Update content with modified soup
            content = str(soup)

        # Save the resource
        await self.save_resource(url, content, content_type)

    async def download_and_save_image(self, img_url, img_tag, parent_url):
        async with self.image_semaphore:
            # Skip if already processed
            if await self.state.is_visited(img_url):
                return
                
            # Mark as visited
            await self.state.add_visited(img_url)
            
            # Download and save
            img_content, img_content_type = await self.download_resource(img_url)
            if img_content is not None:
                await self.save_resource(img_url, img_content, img_content_type)
                
                # Update the img tag's src to a relative path
                new_relative_path = await self.get_relative_path(parent_url, img_url)
                if new_relative_path:
                    img_tag['src'] = new_relative_path
                else:
                    # Fallback for when path isn't in state yet
                    img_tag['src'] = os.path.relpath(self.get_file_path(img_url), os.path.dirname(self.get_file_path(parent_url)))