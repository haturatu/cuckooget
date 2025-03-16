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
from collections import deque

class AsyncWebMirror:
    def __init__(self, start_url, output_dir, max_connections=50, weights=None, excluded_urls=None):
        self.start_url = start_url
        self.output_dir = output_dir
        self.domain = urlparse(start_url).netloc
        self.visited = CuckooHash(100000)  # 完了済みURLを管理
        self.path_map = CuckooHash(100000)  # URLとファイルパスのマッピング
        self.url_to_path = {}  # URLと相対パスのマッピング
        self.max_connections = max_connections
        self.semaphore = asyncio.Semaphore(max_connections)
        self.task_queue = asyncio.PriorityQueue()  # タスクキュー
        self.weights = weights or []  # 優先度の重み
        self.image_semaphore = asyncio.Semaphore(max_connections * 2)
        self.excluded_urls = excluded_urls or []  # 除外するURL
        self.dag = DAG()  # DAG（有向非巡回グラフ）
        self.lock = asyncio.Lock()  # ロック
        self.session = None  # aiohttpセッション
        self.processed_count = 0  # 処理済みURLのカウント
        self.last_20_urls = deque(maxlen=20)  # 最後の20件のURLを保持するキュー

        # 保存済みの状態を読み込む
        self.load_state()

    def load_state(self):
        """状態をロードし、最後の20件のURLをキューに追加する"""
        state_file = os.path.join(self.output_dir, "state")
        if os.path.exists(state_file):
            with open(state_file, "r", encoding="utf-8") as f:
                for line in f:
                    url, path = line.strip().split(" ", 1)
                    self.path_map.insert(url, path)  # パスマップに追加
                    self.url_to_path[url] = path  # URLとパスのマッピング
                    self.last_20_urls.append(url)  # 最後の20件のURLをキューに追加

    def save_state(self):
        """現在の状態をファイルに保存する"""
        state_file = os.path.join(self.output_dir, "state")
        print(f"Saving state to: {state_file}")  # 保存先を出力
        try:
            with open(state_file, "w", encoding="utf-8") as f:
                for url, path in self.url_to_path.items():
                    f.write(f"{url} {path}\n")
        except Exception as e:
            print(f"Error saving state: {e}")

    async def download_resource(self, url):
        """リソースをダウンロードする"""
        async with self.lock:
            if not self.dag.add_node(url):  # DAGにノードを追加
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
                    self.dag.remove_node(url)  # DAGからノードを削除
        return None, None

    def get_file_path(self, url):
        """URLからファイルパスを生成する"""
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

    async def ensure_directory_exists(self, directory_path):
        """ディレクトリが存在することを確認し、必要なら作成する"""
        path_parts = directory_path.split(os.sep)
        current_path = ""
        
        for i, part in enumerate(path_parts):
            if not part:
                current_path = os.sep
                continue
                
            if current_path:
                current_path = os.path.join(current_path, part)
            else:
                current_path = part
                
            if i == len(path_parts) - 1:
                break
                
            if os.path.exists(current_path):
                if not os.path.isdir(current_path):
                    new_name = f"{current_path}_{xxhash.xxh32(current_path.encode()).hexdigest()[:8]}"
                    print(f"Path component is a file, renaming: {current_path} -> {new_name}")
                    os.rename(current_path, new_name)
                    os.makedirs(current_path, exist_ok=True)
            else:
                try:
                    os.makedirs(current_path, exist_ok=True)
                except Exception as e:
                    print(f"Error creating directory {current_path}: {e}")
                    raise

    async def save_resource(self, url, content, content_type):
        """リソースを保存する"""
        file_path = self.get_file_path(url)
        parent_dir = os.path.dirname(file_path)

        try:
            await self.ensure_directory_exists(parent_dir)
            
            if os.path.exists(file_path):
                print(f"File already exists: {file_path}")
                relative_path = os.path.relpath(file_path, self.output_dir)
                self.visited.insert(url, "True")  # 完了済みとしてマーク
                self.path_map.insert(url, relative_path)
                self.url_to_path[url] = relative_path
                return relative_path

            mode = 'w' if content_type.startswith('text') else 'wb'
            encoding = 'utf-8' if content_type.startswith('text') else None

            try:
                async with aiofiles.open(file_path, mode=mode, encoding=encoding) as f:
                    await f.write(content)
            except Exception as e:
                print(f"Error saving {url} to {file_path}: {e}")
                return None

            relative_path = os.path.relpath(file_path, self.output_dir)
            self.visited.insert(url, "True")  # 完了済みとしてマーク
            self.path_map.insert(url, relative_path)
            self.url_to_path[url] = relative_path

            self.processed_count += 1
            if self.processed_count % 10 == 0:
                self.save_state()

            return relative_path
            
        except Exception as e:
            print(f"Error in save_resource for {url}: {e}")
            return None

    def process_links(self, soup, base_url):
        """HTMLからリンクを抽出し、新しいURLを探索する"""
        links = []
        for tag in soup.find_all(['a', 'link', 'script', 'img']):
            attr = 'href' if tag.name in ['a', 'link'] else 'src'
            url = tag.get(attr)
            if url:
                full_url = urljoin(base_url, url)
                if self.domain in full_url and not self.visited.get(full_url) and not any(excluded in full_url for excluded in self.excluded_urls):
                    priority = self.get_url_priority(full_url)
                    links.append((priority, full_url, (tag.name, attr)))
        return links

    def get_url_priority(self, url):
        """URLの優先度を取得する"""
        for i, weight in enumerate(self.weights):
            if weight in url:
                return i
        return len(self.weights)

    def get_relative_path(self, from_url, to_url):
        # 新しいデータ構造からパスを取得
        from_path = self.url_to_path.get(from_url)
        to_path = self.url_to_path.get(to_url)
        if from_path and to_path:
            return os.path.relpath(to_path, os.path.dirname(from_path))
        return None

    async def mirror_site(self):
        """サイトのミラーリングを開始する"""
        # 最後の20件のURLをキューに追加
        for url in self.last_20_urls:
            await self.task_queue.put((0, 0, url, None))

        async with aiohttp.ClientSession(json_serialize=ujson.dumps) as self.session:
            tasks = set()
            while not self.task_queue.empty() or tasks:
                while len(tasks) < self.max_connections and not self.task_queue.empty():
                    _, count, url, tag_info = await self.task_queue.get()

                    async with self.lock:
                        if not self.visited.get(url):  # 未処理のURLのみ処理
                            task = asyncio.create_task(self.process_url(url, tag_info, count))
                            tasks.add(task)

                if tasks:
                    done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        try:
                            await task
                        except Exception as e:
                            print(f"Error in task: {e}")

        self.save_state()

    async def process_url(self, url, tag_info, count):
        """URLを処理する"""
        print(f"Downloading: {url}")
        content, content_type = await self.download_resource(url)
        if content is None:
            return

        if content_type.startswith('text/html'):
            soup = BeautifulSoup(content, 'html.parser')

            img_tasks = []
            for img_tag in soup.find_all('img', src=True):
                img_url = urljoin(url, img_tag['src'])
                if not self.visited.get(img_url):  # 未処理の画像のみ処理
                    img_tasks.append(self.download_and_save_image(img_url, img_tag, url))

            links = self.process_links(soup, url)
            async with self.lock:
                for priority, new_url, new_tag_info in links:
                    if self.dag.add_edge(url, new_url) and not self.visited.get(new_url):  # 未処理のURLのみキューに追加
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
        if relative_path:
            async with self.lock:
                self.visited.insert(url, "True")  # 完了済みとしてマーク

        if tag_info and self.weights:
            tag_name, attr = tag_info
            soup = BeautifulSoup('', 'html.parser')
            tag = soup.new_tag(tag_name)
            tag[attr] = relative_path
            print(f"Updated tag: {tag}")

    async def download_and_save_image(self, img_url, img_tag, parent_url):
        """画像をダウンロードして保存する"""
        async with self.image_semaphore:
            if self.visited.get(img_url):  # 完了済みの画像はスキップ
                return
            img_content, img_content_type = await self.download_resource(img_url)
            if img_content is not None:
                async with self.lock:
                    if not self.visited.get(img_url):  # 未処理の画像のみ保存
                        img_relative_path = await self.save_resource(img_url, img_content, img_content_type)
                        if img_relative_path:
                            self.visited.insert(img_url, "True")  # 完了済みとしてマーク

                            new_relative_path = self.get_relative_path(parent_url, img_url)
                            if new_relative_path:
                                img_tag['src'] = new_relative_path
                            else:
                                img_tag['src'] = os.path.relpath(self.get_file_path(img_url), os.path.dirname(self.get_file_path(parent_url)))

