import asyncio
from curl_cffi import AsyncSession
import aiofiles
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import xxhash
from cuckoo_nest import CuckooHash, DAG
import ujson
import json
import time

class AsyncWebMirror:
    def __init__(self, start_url, output_dir, max_connections=50, weights=None, excluded_urls=None):
        self.start_url = start_url
        self.output_dir = output_dir
        self.domain = urlparse(start_url).netloc
        self.visited = CuckooHash(100000)
        self.path_map = CuckooHash(100000)
        self.visited_urls = set()
        self.completed_urls = set()
        self.url_to_path = {}
        self.max_connections = max_connections
        self.semaphore = asyncio.Semaphore(max_connections)
        self.task_queue = asyncio.PriorityQueue()
        self.weights = weights or []
        self.image_semaphore = asyncio.Semaphore(max_connections * 2)
        self.excluded_urls = excluded_urls or []
        self.dag = DAG()
        self.lock = asyncio.Lock()
        self.session = None
        self.processed_count = 0
        self.state_save_interval = 5
        self.last_state_save_time = time.time()
        self.state_save_time_interval = 30
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # State file paths
        self.state_file = os.path.join(self.output_dir, "state.json")
        self.temp_state_file = os.path.join(self.output_dir, "state.json.tmp")
        self.completion_flag_file = os.path.join(self.output_dir, "COMPLETE")
        
        # Load previous state
        self.load_state()

    def load_state(self):
        """Load previous download state"""
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
            
            # Load sets of visited and completed URLs
            self.visited_urls = set(state_data.get('visited_urls', []))
            self.completed_urls = set(state_data.get('completed_urls', []))
            
            # Update CuckooHash structures
            for url, path in self.url_to_path.items():
                self.visited.insert(url, "True")
                self.path_map.insert(url, path)
                
            print(f"Loaded previous state: {len(self.visited_urls)} visited URLs, {len(self.completed_urls)} completed.")
            
            # Identify URLs that were visited but not completed
            pending_urls = self.visited_urls - self.completed_urls
            if pending_urls:
                print(f"Found {len(pending_urls)} URLs that need to be reprocessed.")
        except Exception as e:
            print(f"Error loading previous state: {e}")
            # If state is corrupted, start fresh
            self.url_to_path = {}
            self.visited_urls = set()
            self.completed_urls = set()

    def save_state(self, force=False):
        """Save current download state to a file"""
        current_time = time.time()
        
        # Save state if forced, or if processed_count is a multiple of state_save_interval,
        # or if enough time has passed since the last save
        if (not force and 
            self.processed_count % self.state_save_interval != 0 and
            current_time - self.last_state_save_time < self.state_save_time_interval):
            return
            
        self.last_state_save_time = current_time
            
        try:
            # First write to a temporary file
            state_data = {
                'url_to_path': self.url_to_path,
                'visited_urls': list(self.visited_urls),
                'completed_urls': list(self.completed_urls)
            }
            
            with open(self.temp_state_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f)
                
            # Then rename to the actual state file (atomic operation)
            os.replace(self.temp_state_file, self.state_file)
            
            if force:
                print(f"State saved: {len(self.visited_urls)} visited, {len(self.completed_urls)} completed.")
        except Exception as e:
            print(f"Error saving state: {e}")

    def mark_download_complete(self):
        """Mark the download as completely finished"""
        # Save final state
        self.save_state(force=True)
        
        # Create completion flag file
        with open(self.completion_flag_file, 'w', encoding='utf-8') as f:
            f.write(f"Download completed at {time.ctime()}\n")
            f.write(f"Total URLs processed: {len(self.completed_urls)}")
            
        print(f"Download marked as complete. Total URLs: {len(self.completed_urls)}")

    async def download_resource(self, url):
        async with self.lock:
            if not self.dag.add_node(url):  # DAG node already exists
                return None, None

        max_retries = 5
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                async with self.semaphore:
                    response = await self.session.get(url, impersonate="chrome110", timeout=3)
                    if response.status_code == 200:
                        content_type = response.headers.get('content-type', '').split(';')[0]
                        if content_type.startswith('text') or url.endswith(('.php', '.pl')):
                            return response.text, 'text/html'
                        else:
                            return response.content, content_type
                    elif 500 <= response.status_code < 600:
                        if attempt < max_retries - 1:
                            print(f"Retrying {url} due to status {response.status_code} (attempt {attempt + 1})")
                            await asyncio.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                        else:
                            print(f"Failed to download {url} after {max_retries} attempts: Status {response.status_code}")
                            return None, None
                    else:
                        print(f"Error downloading {url}: Status {response.status_code}")
                        return None, None
            except asyncio.TimeoutError:
                print(f"Timeout error for {url}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                    continue
            except Exception as e:
                print(f"Error downloading {url}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                    continue
            finally:
                async with self.lock:
                    self.dag.remove_node(url)
        return None, None

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
        max_filename_length = 255  # Filesystem limit
        if len(os.path.basename(path)) > max_filename_length:
            filename_hash = xxhash.xxh32(path.encode()).hexdigest()[:16]
            base, ext = os.path.splitext(path)
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

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        mode = 'w' if content_type.startswith('text') else 'wb'
        encoding = 'utf-8' if content_type.startswith('text') else None

        # If file already exists, update mappings but don't rewrite
        if os.path.exists(file_path):
            print(f"File already exists: {file_path}")
            relative_path = os.path.relpath(file_path, self.output_dir)
            self.visited.insert(url, "True")
            self.path_map.insert(url, relative_path)
            self.url_to_path[url] = relative_path
            
            # Mark as completed
            self.completed_urls.add(url)
            
            # Update counters and save state if needed
            self.processed_count += 1
            self.save_state()
            
            return relative_path

        try:
            async with aiofiles.open(file_path, mode=mode, encoding=encoding) as f:
                await f.write(content)
        except Exception as e:
            print(f"Error saving {url}: {e}")
            return None

        relative_path = os.path.relpath(file_path, self.output_dir)
        self.visited.insert(url, "True")
        self.path_map.insert(url, relative_path)
        self.url_to_path[url] = relative_path
        
        # Mark as completed
        self.completed_urls.add(url)
        
        # Update counters and save state if needed
        self.processed_count += 1
        self.save_state()

        return relative_path

    def process_links(self, soup, base_url):
        links = []
        for tag in soup.find_all(['a', 'link', 'script', 'img']):
            attr = 'href' if tag.name in ['a', 'link'] else 'src'
            url = tag.get(attr)
            if url:
                if url.startswith('#'):
                    continue
                full_url = urljoin(base_url, url)
                if (self.domain in full_url and 
                    full_url not in self.visited_urls and 
                    not any(excluded in full_url for excluded in self.excluded_urls)):
                    priority = self.get_url_priority(full_url)
                    links.append((priority, full_url, (tag.name, attr)))
        return links

    def get_url_priority(self, url):
        for i, weight in enumerate(self.weights):
            if weight in url:
                return i  # Lower index = higher priority
        return len(self.weights)  # Default lowest priority

    def get_relative_path(self, from_url, to_url):
        from_path = self.url_to_path.get(from_url)
        to_path = self.url_to_path.get(to_url)
        if from_path and to_path:
            return os.path.relpath(to_path, os.path.dirname(from_path))
        return None

    async def mirror_site(self):
        # Check if download was previously completed
        if os.path.exists(self.completion_flag_file):
            print("Download was already completed. Use --force to download again.")
            return
            
        # Add pending URLs from previous run to the queue first
        pending_urls = self.visited_urls - self.completed_urls
        if pending_urls:
            print(f"Resuming download of {len(pending_urls)} pending URLs")
            for url in pending_urls:
                priority = self.get_url_priority(url)
                await self.task_queue.put((priority, 0, url, None))

        # Add start URL if not already in the queue
        if not pending_urls or self.start_url not in self.visited_urls:
            await self.task_queue.put((0, 0, self.start_url, None))

        async with AsyncSession() as session:
            self.session = session
            tasks = set()
            try:
                while not self.task_queue.empty() or tasks:
                    # Process URLs while maintaining concurrency limits
                    while len(tasks) < self.max_connections and not self.task_queue.empty():
                        _, count, url, tag_info = await self.task_queue.get()

                        # Avoid reprocessing already completed URLs
                        if url in self.completed_urls:
                            continue

                        # Avoid processing already visited urls
                        if url in self.visited_urls:
                            continue
                            
                        async with self.lock:
                            # Mark as visited before processing
                            self.visited.insert(url, "True")
                            self.visited_urls.add(url)
                            self.save_state()  # Save state when adding new URLs
                        
                        task = asyncio.create_task(self.process_url(url, tag_info, count))
                        tasks.add(task)

                    if tasks:
                        # Wait for at least one task to complete
                        done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                        for task in done:
                            try:
                                await task
                            except Exception as e:
                                print(f"Task error: {e}")
            except KeyboardInterrupt:
                print("Download interrupted. Progress saved. You can resume later.")
                # Ensure state is saved on keyboard interrupt
                self.save_state(force=True)
                return
            except Exception as e:
                print(f"Error during mirroring: {e}")
                # Ensure state is saved on error
                self.save_state(force=True)
                raise

        # Mark download as complete
        self.mark_download_complete()

    async def process_url(self, url, tag_info, count):
        print(f"Downloading: {url}")
        content, content_type = await self.download_resource(url)
        if content is None:
            # If download failed, don't mark as completed but still as visited
            print(f"Failed to download: {url}")
            return

        if content_type.startswith('text/html'):
            soup = BeautifulSoup(content, 'html.parser')

            # Collect and process image tasks
            img_tasks = []
            for img_tag in soup.find_all('img', src=True):
                img_url = urljoin(url, img_tag['src'])
                if img_url not in self.visited_urls and img_url not in self.completed_urls:
                    img_tasks.append(self.download_and_save_image(img_url, img_tag, url))

            # Process links and add to queue
            links = self.process_links(soup, url)
            async with self.lock:
                for priority, new_url, new_tag_info in links:
                    if self.dag.add_edge(url, new_url) and new_url not in self.visited_urls:
                        await self.task_queue.put((priority, count + 1, new_url, new_tag_info))

            # Rewrite links to use relative paths
            for new_tag in soup.find_all(['a', 'link', 'img', 'script']):
                attr = 'href' if new_tag.name in ['a', 'link'] else 'src'
                if new_tag.get(attr):
                    new_full_url = urljoin(url, new_tag[attr])
                    new_relative_path = self.get_relative_path(url, new_full_url)
                    if new_relative_path:
                        new_tag[attr] = new_relative_path
                    elif self.domain in new_full_url:
                        new_path = os.path.relpath(self.get_file_path(new_full_url), os.path.dirname(self.get_file_path(url)))
                        new_tag[attr] = new_path

            # Special handling for PHP/PL links
            for a_tag in soup.find_all('a', href=True):
                if a_tag['href'].endswith(('.php', '.pl')):
                    a_tag['href'] = a_tag['href'].rsplit('.', 1)[0] + '.html'

            # Wait for all image tasks to complete
            await asyncio.gather(*img_tasks)

            # Update content with modified soup
            content = str(soup)

        # Save the resource
        relative_path = await self.save_resource(url, content, content_type)

        # Update tag if needed
        if tag_info and self.weights:
            tag_name, attr = tag_info
            soup = BeautifulSoup('', 'html.parser')
            tag = soup.new_tag(tag_name)
            tag[attr] = relative_path
            print(f"Updated tag: {tag}")

    async def download_and_save_image(self, img_url, img_tag, parent_url):
        async with self.image_semaphore:
            # Skip if already processed
            if img_url in self.visited_urls:
                return
                
            # Mark as visited
            async with self.lock:
                self.visited_urls.add(img_url)
                self.visited.insert(img_url, "True")
            
            # Download and save
            img_content, img_content_type = await self.download_resource(img_url)
            if img_content is not None:
                # Save the image
                img_relative_path = await self.save_resource(img_url, img_content, img_content_type)
                
                # Update the img tag
                new_relative_path = self.get_relative_path(parent_url, img_url)
                if new_relative_path:
                    img_tag['src'] = new_relative_path
                else:
                    img_tag['src'] = os.path.relpath(self.get_file_path(img_url), os.path.dirname(self.get_file_path(parent_url)))
