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

class AsyncWebMirror:
    def __init__(self, start_url, output_dir, max_connections=50, weights=None, excluded_urls=None):
        self.start_url = start_url
        self.output_dir = output_dir
        self.domain = urlparse(start_url).netloc
        self.visited = CuckooHash(100000)
        self.path_map = CuckooHash(100000)
        self.visited_urls = set()  # CuckooHashの代わりに通常のセットを使用して訪問済みURLを記録
        self.url_to_path = {}  # URLとパスのマッピングを保持
        self.max_connections = max_connections
        self.semaphore = asyncio.Semaphore(max_connections)
        self.task_queue = asyncio.PriorityQueue()
        self.weights = weights or []
        self.image_semaphore = asyncio.Semaphore(max_connections * 2)
        self.excluded_urls = excluded_urls or []
        self.dag = DAG()
        self.lock = asyncio.Lock()
        self.session = None
        self.processed_count = 0  # ダウンロード処理回数をカウント

        # 保存済みの状態を読み込む
        self.load_state()

    def load_state(self):
        """保存済みの状態をファイルから読み込む"""
        state_file = os.path.join(self.output_dir, "state")
        if os.path.exists(state_file):
            with open(state_file, "r", encoding="utf-8") as f:
                for line in f:
                    url, path = line.strip().split(" ", 1)
                    self.visited.insert(url, "True")
                    self.path_map.insert(url, path)
                    # 新しいデータ構造にも追加
                    self.visited_urls.add(url)
                    self.url_to_path[url] = path

    def save_state(self):
        """現在の状態をファイルに保存する"""
        state_file = os.path.join(self.output_dir, "state")
        print(f"Saving state to: {state_file}")  # 保存先を出力
        try:
            with open(state_file, "w", encoding="utf-8") as f:
                # 新しいデータ構造からデータを保存
                for url, path in self.url_to_path.items():
                    f.write(f"{url} {path}\n")
        except Exception as e:
            print(f"Error saving state: {e}")

    async def download_resource(self, url):
        async with self.lock:
            if not self.dag.add_node(url):  # DAGにノードを追加できない場合、すでに処理されている
                return None, None

        max_retries = 5
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                async with self.semaphore:
                    async with self.session.get(url, timeout=300) as response:
                        if response.status == 200:
                            content_type = response.headers.get('content-type', '').split(';')[0]
                            if content_type.startswith('text') or url.endswith(('.php', '.pl')):
                                return await response.text(), 'text/html'
                            else:
                                return await response.read(), content_type
                        elif 500 <= response.status < 600:
                            if attempt < max_retries - 1:
                                print(f"Retrying {url} due to status {response.status} (attempt {attempt + 1})")
                                await asyncio.sleep(retry_delay)
                                retry_delay *= 2
                                continue
                            else:
                                print(f"Failed to download {url} after {max_retries} attempts: Status {response.status}")
                                return None, None
                        else:
                            print(f"Error downloading {url}: Status {response.status}")
                            return None, None
            except asyncio.TimeoutError:
                print(f"Timeout error for {url}")
            except aiohttp.ClientPayloadError as e:
                print(f"Payload error for {url}: {e}")
                return None, None
            except Exception as e:
                print(f"Error downloading {url}: {e}")
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

        # ファイル名が長すぎる場合、ハッシュ化して短縮する
        max_filename_length = 255  # FSの制限
        if len(os.path.basename(path)) > max_filename_length:
            filename_hash = xxhash.xxh32(path.encode()).hexdigest()[:16]
            base, ext = os.path.splitext(path)
            path = f"{filename_hash}{ext}"

        return os.path.join(self.output_dir, parsed_url.netloc, path.lstrip('/'))

    async def save_resource(self, url, content, content_type):
        file_path = self.get_file_path(url)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        mode = 'w' if content_type.startswith('text') else 'wb'
        encoding = 'utf-8' if content_type.startswith('text') else None

        # 既にファイルが存在する場合でも、visitedとpath_mapを更新
        if os.path.exists(file_path):
            print(f"File already exists: {file_path}")
            relative_path = os.path.relpath(file_path, self.output_dir)
            self.visited.insert(url, "True")
            self.path_map.insert(url, relative_path)
            # 新しいデータ構造も更新
            self.visited_urls.add(url)
            self.url_to_path[url] = relative_path

            # ダウンロード処理回数をカウントし、10回ごとに状態を保存
            self.processed_count += 1
            if self.processed_count % 10 == 0:
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
        # 新しいデータ構造も更新
        self.visited_urls.add(url)
        self.url_to_path[url] = relative_path

        # ダウンロード処理回数をカウントし、10回ごとに状態を保存
        self.processed_count += 1
        if self.processed_count % 10 == 0:
            self.save_state()

        return relative_path

    def process_links(self, soup, base_url):
        links = []
        for tag in soup.find_all(['a', 'link', 'script', 'img']):
            attr = 'href' if tag.name in ['a', 'link'] else 'src'
            url = tag.get(attr)
            # URLが既に処理されているか、除外されているかを確認
            if url:
                full_url = urljoin(base_url, url)
                # 新しいデータ構造を使用
                if self.domain in full_url and full_url not in self.visited_urls and not any(excluded in full_url for excluded in self.excluded_urls):
                    priority = self.get_url_priority(full_url)
                    links.append((priority, full_url, (tag.name, attr)))
        return links

    def get_url_priority(self, url):
        for i, weight in enumerate(self.weights):
            if weight in url:
                return i  # weightのインデックスを優先度として返す
        return len(self.weights)  # weightがなければ最低優先度

    def get_relative_path(self, from_url, to_url):
        # 新しいデータ構造からパスを取得
        from_path = self.url_to_path.get(from_url)
        to_path = self.url_to_path.get(to_url)
        if from_path and to_path:
            return os.path.relpath(to_path, os.path.dirname(from_path))
        return None

    async def mirror_site(self):
        await self.task_queue.put((0, 0, self.start_url, None))

        async with aiohttp.ClientSession(json_serialize=ujson.dumps) as self.session:
            tasks = set()
            while not self.task_queue.empty() or tasks:
                while len(tasks) < self.max_connections and not self.task_queue.empty():
                    _, count, url, tag_info = await self.task_queue.get()

                    # 重複防止のため、ここでvisitedに挿入してから処理を進める
                    async with self.lock:
                        # 新しいデータ構造を使用して確認
                        if url not in self.visited_urls:
                            self.visited.insert(url, "True")  # CuckooHashにも保存
                            self.visited_urls.add(url)  # 新しいデータ構造に追加
                            task = asyncio.create_task(self.process_url(url, tag_info, count))
                            tasks.add(task)

                if tasks:
                    done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        await task

        # 状態を保存
        self.save_state()

    async def process_url(self, url, tag_info, count):
        print(f"Downloading: {url}")
        content, content_type = await self.download_resource(url)
        if content is None:
            return

        if content_type.startswith('text/html'):
            soup = BeautifulSoup(content, 'html.parser')

            img_tasks = []
            for img_tag in soup.find_all('img', src=True):
                img_url = urljoin(url, img_tag['src'])
                # 新しいデータ構造を使用して確認
                if img_url not in self.visited_urls:
                    img_tasks.append(self.download_and_save_image(img_url, img_tag, url))

            links = self.process_links(soup, url)
            async with self.lock:
                for priority, new_url, new_tag_info in links:
                    # 新しいデータ構造を使用して確認
                    if self.dag.add_edge(url, new_url) and new_url not in self.visited_urls:
                        await self.task_queue.put((priority, count + 1, new_url, new_tag_info))

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

            for a_tag in soup.find_all('a', href=True):
                if a_tag['href'].endswith(('.php', '.pl')):
                    a_tag['href'] = a_tag['href'].rsplit('.', 1)[0] + '.html'

            await asyncio.gather(*img_tasks)

            content = str(soup)

        relative_path = await self.save_resource(url, content, content_type)

        if tag_info and self.weights:
            tag_name, attr = tag_info
            soup = BeautifulSoup('', 'html.parser')
            tag = soup.new_tag(tag_name)
            tag[attr] = relative_path
            print(f"Updated tag: {tag}")

    async def download_and_save_image(self, img_url, img_tag, parent_url):
        async with self.image_semaphore:
            # 新しいデータ構造を使用して確認
            if img_url in self.visited_urls:  # 画像が既に処理されている場合は処理をスキップ
                return
            img_content, img_content_type = await self.download_resource(img_url)
            if img_content is not None:
                # 重複チェック後に保存処理を行う
                async with self.lock:
                    # 新しいデータ構造を使用して確認
                    if img_url not in self.visited_urls:  # 二重チェック
                        img_relative_path = await self.save_resource(img_url, img_content, img_content_type)
                        self.visited.insert(img_url, "True")  # CuckooHashにも保存
                        self.visited_urls.add(img_url)  # 新しいデータ構造に追加

                        # 親URLとの相対パスを取得してimgタグを更新
                        new_relative_path = self.get_relative_path(parent_url, img_url)
                        if new_relative_path:
                            img_tag['src'] = new_relative_path
                        else:
                            img_tag['src'] = os.path.relpath(self.get_file_path(img_url), os.path.dirname(self.get_file_path(parent_url)))

