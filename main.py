#!/usr/bin/python3

import argparse
import asyncio
import aiohttp
from aiohttp import ClientSession
import aiofiles
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import hashlib
import random

class CuckooHash:
    def __init__(self, size):
        self.size = size
        self.table = [None] * size
        self.max_kicks = 500

    def hash1(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.size

    def hash2(self, key):
        return int(hashlib.sha256(key.encode()).hexdigest(), 16) % self.size

    def insert(self, key, value):
        for _ in range(self.max_kicks):
            key, value = self._insert(key, value)
            if key is None:
                return True
        return False

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
        elif self.table[h2] and self.table[h2][0] == key:
            self.table[h2] = None

class AsyncWebMirror:
    def __init__(self, start_url, output_dir, max_connections=50, weights=None):
        self.start_url = start_url
        self.output_dir = output_dir
        self.domain = urlparse(start_url).netloc
        self.visited = CuckooHash(10000)
        self.downloading = CuckooHash(1000)
        self.path_map = CuckooHash(10000)
        self.max_connections = max_connections
        self.semaphore = asyncio.Semaphore(max_connections)
        self.task_queue = asyncio.PriorityQueue()
        self.weights = weights or []
        self.image_semaphore = asyncio.Semaphore(max_connections * 2)

    async def download_resource(self, url, session):
        if self.downloading.get(url):
            return None, None

        self.downloading.insert(url, True)
        try:
            async with self.semaphore:
                async with session.get(url, timeout=300) as response:
                    if response.status == 200:
                        content_type = response.headers.get('content-type', '').split(';')[0]
                        if content_type.startswith('text') or url.endswith('.php'):
                            return await response.text(), 'text/html'
                        else:
                            return await response.read(), content_type
                    else:
                        print(f"Error downloading {url}: Status {response.status}")
                        return None, None
        except asyncio.TimeoutError:
            print(f"Timeout error for {url}")
            return None, None
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            return None, None
        finally:
            self.downloading.remove(url)

    def get_file_path(self, url):
        parsed_url = urlparse(url)
        path = parsed_url.path
        query = parsed_url.query

        if not path or path.endswith('/'):
            path = os.path.join(path, 'index.html')
        elif '.' not in os.path.basename(path) or path.endswith('.php'):
            path = f"{path}.html"

        if query:
            query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
            base, ext = os.path.splitext(path)
            path = f"{base}_{query_hash}{ext}"

        return os.path.join(self.output_dir, parsed_url.netloc, path.lstrip('/'))

    async def save_resource(self, url, content, content_type):
        file_path = self.get_file_path(url)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        mode = 'w' if content_type.startswith('text') else 'wb'
        encoding = 'utf-8' if content_type.startswith('text') else None

        try:
            async with aiofiles.open(file_path, mode=mode, encoding=encoding) as f:
                await f.write(content)
        except Exception as e:
            print(f"Error saving {url}: {e}")

        relative_path = os.path.relpath(file_path, self.output_dir)
        self.path_map.insert(url, relative_path)
        return relative_path

    def process_links(self, soup, base_url):
        for tag in soup.find_all(['a', 'link', 'script']):
            attr = 'href' if tag.name in ['a', 'link'] else 'src'
            url = tag.get(attr)
            if url:
                full_url = urljoin(base_url, url)
                if self.domain in full_url and not self.visited.get(full_url):
                    priority = self.get_url_priority(full_url)
                    yield priority, full_url, (tag.name, attr)

    def get_url_priority(self, url):
        for i, weight in enumerate(self.weights):
            if weight in url:
                return i
        return len(self.weights)

    def get_relative_path(self, from_url, to_url):
        from_path = self.path_map.get(from_url)
        to_path = self.path_map.get(to_url)
        if from_path and to_path:
            return os.path.relpath(to_path, os.path.dirname(from_path))
        return None

    async def mirror_site(self):
        await self.task_queue.put((0, 0, self.start_url, None))

        async with aiohttp.ClientSession() as session:
            tasks = set()
            while not self.task_queue.empty() or tasks:
                while len(tasks) < self.max_connections and not self.task_queue.empty():
                    _, count, url, tag_info = await self.task_queue.get()
                    if not self.visited.get(url):
                        task = asyncio.create_task(self.process_url(url, tag_info, session, count))
                        tasks.add(task)

                if tasks:
                    done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        await task

    async def process_url(self, url, tag_info, session, count):
        if self.visited.get(url):
            return

        print(f"Downloading: {url}")
        content, content_type = await self.download_resource(url, session)
        if content is None:
            return

        self.visited.insert(url, True)

        if content_type.startswith('text/html'):
            soup = BeautifulSoup(content, 'html.parser')

            img_tasks = []
            for img_tag in soup.find_all('img', src=True):
                img_url = urljoin(url, img_tag['src'])
                if not self.visited.get(img_url):
                    img_tasks.append(self.download_and_save_image(img_url, img_tag, session, url))

            for priority, new_url, new_tag_info in self.process_links(soup, url):
                await self.task_queue.put((priority, count + 1, new_url, new_tag_info))

            for new_tag in soup.find_all(['a', 'link', 'img', 'script']):
                attr = 'href' if new_tag.name in ['a', 'link'] else 'src'
                if new_tag.get(attr):
                    new_full_url = urljoin(url, new_tag[attr])
                    if self.domain in new_full_url:
                        new_relative_path = self.get_relative_path(url, new_full_url)
                        if new_relative_path:
                            new_tag[attr] = new_relative_path

            for a_tag in soup.find_all('a', href=True):
                if a_tag['href'].endswith('.php'):
                    a_tag['href'] = a_tag['href'].rsplit('.', 1)[0] + '.html'

            await asyncio.gather(*img_tasks)

            content = str(soup)

        relative_path = await self.save_resource(url, content, content_type)

        if tag_info:
            tag_name, attr = tag_info
            soup = BeautifulSoup('', 'html.parser')
            tag = soup.new_tag(tag_name)
            tag[attr] = relative_path
            print(f"Updated tag: {tag}")

    async def download_and_save_image(self, img_url, img_tag, session, parent_url):
        async with self.image_semaphore:
            if not self.visited.get(img_url):
                img_content, img_content_type = await self.download_resource(img_url, session)
                if img_content is not None:
                    img_relative_path = await self.save_resource(img_url, img_content, img_content_type)
                    new_relative_path = self.get_relative_path(parent_url, img_url)
                    if new_relative_path:
                        img_tag['src'] = new_relative_path

async def main():
    parser = argparse.ArgumentParser(description='ウェブサイトをミラーリングします。')
    parser.add_argument('url', help='ミラーリングするウェブサイトのURL')
    parser.add_argument('output_dir', help='ミラーリングしたファイルを保存するディレクトリ')
    parser.add_argument('-c', '--connections', type=int, default=50, help='同時接続数 (デフォルト: 50)')
    parser.add_argument('-w', '--weights', nargs='+', help='URLの優先度を設定する文字列（スペース区切りで複数指定可能）')
    args = parser.parse_args()

    mirror = AsyncWebMirror(args.url, args.output_dir, max_connections=args.connections, weights=args.weights)
    await mirror.mirror_site()
    print("ミラーリングが完了しました。")

if __name__ == "__main__":
    asyncio.run(main())

