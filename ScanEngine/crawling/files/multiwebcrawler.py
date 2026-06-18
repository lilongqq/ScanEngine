import asyncio
import hashlib
import io
import logging
import threading
import traceback
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

import nest_asyncio
from bs4 import BeautifulSoup
from exceptiongroup import catch
from crawling.interface import Iterator, Client
from common.functools import gen_key
from pyppeteer import launch
from pyppeteer.errors import NetworkError, PageError, TimeoutError
from urllib.parse import urlparse
from collections import UserDict
from re import sub, compile, I
import lxml_html_clean
from functools import partial
from requests_html import HTMLSession, AsyncHTMLSession


class WebURLDict(UserDict):

    def __getitem__(self, item):
        if item == 'timestamp':
            return int(self.data['index_time'])
        elif item == 'diff':
            return gen_key(
                name=self.data.get('name'),
                path=self.data.get('path'),
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == 'timestamp':
            return 'index_time' in self.data
        elif item == 'diff':
            return {'name', 'path'}.issubset(self.data)
        else:
            return item in self.data

class WebURL(Client):
    def __init__(self,max_level):
        self.logger = logging.getLogger(self.__class__.__name__)
        # self.loop = asyncio.new_event_loop()
        # asyncio.set_event_loop(self.loop)
        # self.multi_session = AsyncHTMLSession()
        nest_asyncio.apply()  # 解决事件循环冲突
        self.session = HTMLSession()
        self.session.browser  # 预初始化浏览器实例
        self.render_lock = threading.Lock()  # 渲染锁
        self.headers = {
            'User-Agent': 'Mozilla/5.0'
        }
        self.max_level = max_level
        self._init_browser()

    def _init_browser(self):
        if not hasattr(self.session, 'browser'):
            self.session.browser = self.session.loop.run_until_complete(
                launch(headless=True)
            )

    async def async_render(self, url, response, timeout):
        try:
            await response.html.arender(scrolldown=3, timeout=timeout)
            # response.html.render(scrolldown=3, timeout=timeout,keep_page=True,sleep=1)
        except Exception as e:
            # self.logger.error(f"Render failed: {str(e)}, the url is: {str(url)}")
            pass

    # async def async_render_v1(self, url, response):
    #     loop = asyncio.new_event_loop()
    #     session = AsyncHTMLSession()
    #     response = await session.get(url, headers=self.headers)
    #     try:
    #         loop.run_until_complete(
    #             response.html.arender(
    #                 scrolldown=3,
    #                 timeout=15,
    #                 wait=0.5
    #             )
    #         )
    #         return response
    #     finally:
    #         pass
    #     #     loop.close()
    #     #     session.close()

    # async def pyppeteer_render(self, url):
    #     browser = await launch(headless=True)
    #     page = await browser.newPage()
    #     try:
    #         await page.goto(url, timeout=15000)
    #         await page.waitFor(2000)  # 显式等待
    #         content = await page.content()
    #         return BeautifulSoup(content, 'html.parser')
    #     finally:
    #         await browser.close()

    def thread_safe_render(self, url):
        import nest_asyncio
        nest_asyncio.apply()

        async def _render():
            session = AsyncHTMLSession()
            response = await session.get(url,headers=self.headers)
            await response.html.arender(timeout=5)
            return response

        return asyncio.run(_render())

    def get_nodes(self,url,level):
        max_retries = 5
        timeout = 10
        while max_retries > 0:
            try:
                response = self.session.get(url, headers=self.headers)
                if self.need_js_rendering(response.text):
                    response.html.render(scrolldown=3, timeout=timeout)
                max_retries = max_retries + 1
                break
            except (PageError, NetworkError, TimeoutError) as e:
                self.logger.error(f'{type(e).__name__} => level {level}: {url}, retrying')
                time.sleep(2)
                max_retries -= 1
        if max_retries == 0:
            self.logger.error('================ faile to connect the page: {} ================='.format(url))
            # raise ConnectionError

        # soup = BeautifulSoup(driver.page_source, 'html.parser')
        soup = BeautifulSoup(response.html.html, 'html.parser')
        link_list = set()
        parsed_target_url = urlparse(url)
        if(level < self.max_level):
            for link in soup.find_all('a', href=True):
                if link['href'].startswith('/'):
                    link_list.add('{}://{}{}'.format(parsed_target_url.scheme, parsed_target_url.netloc, link['href']))
                elif link['href'].startswith('./'):
                    link_list.add('{}{}'.format(url, link['href'].removeprefix('./')))
                elif self.url_filter(parsed_target_url.netloc, link['href']):
                    link_list.add(link['href'])

        for element in soup(['script', 'style', 'nav', 'footer', 'aside']):
            element.decompose()
        article = soup.find('article') or \
                  soup.find('div', class_=['content', 'article-body']) or \
                  soup.find('main') or \
                  soup.find('div', id=['content', 'main'])
        text = sub(r'\n+', '\n', article.get_text(separator='\n', strip=True) if article else soup.get_text())

        url_details = {
            'url':url,
            'sub_link_list':link_list,
            'text_content': text
        }
        return url_details,level+1

    def multi_get_nodes(self,url,level):
        max_retries = 3
        while max_retries > 0:
            try:
                response = self.session.get(url, headers=self.headers)
                if self.need_js_rendering(response.text):
                    nest_asyncio.apply()
                    asyncio.run(self.async_render(url, response, timeout=10))
                #     # self.async_render(url, response, timeout=10)
                #     # response.html.render(scrolldown=3, timeout=10)
                # response = self.thread_safe_render(url)
                max_retries = max_retries + 1
                break
            except (PageError, NetworkError, TimeoutError) as e:
                self.logger.error(f'{type(e).__name__} => level {level}: {url}, retrying')
                time.sleep(2)
                max_retries -= 1
        if max_retries == 0:
            self.logger.error('================ faile to connect the page: {} ================='.format(url))
            url_details = {
                'url': url,
                'sub_link_list': [],
                'text_content': ''
            }
            return url_details, level + 1
            # raise ConnectionError

        # soup = BeautifulSoup(driver.page_source, 'html.parser')
        soup = BeautifulSoup(response.html.html, 'html.parser')
        link_list = set()
        parsed_target_url = urlparse(url)
        if(level < self.max_level):
            for link in soup.find_all('a', href=True):
                if link['href'].startswith('/'):
                    link_list.add('{}://{}{}'.format(parsed_target_url.scheme, parsed_target_url.netloc, link['href']))
                elif link['href'].startswith('./'):
                    link_list.add('{}{}'.format(url, link['href'].removeprefix('./')))
                elif self.url_filter(parsed_target_url.netloc, link['href']):
                    link_list.add(link['href'])

        for element in soup(['script', 'style', 'nav', 'footer', 'aside']):
            element.decompose()
        article = soup.find('article') or \
                  soup.find('div', class_=['content', 'article-body']) or \
                  soup.find('main') or \
                  soup.find('div', id=['content', 'main'])
        text = sub(r'\n+', '\n', article.get_text(separator='\n', strip=True) if article else soup.get_text())

        url_details = {
            'url':url,
            'sub_link_list':link_list,
            'text_content': text
        }
        return url_details,level+1

    def url_filter(self, domain, target_url):
        parsed_target_url = urlparse(target_url)
        if parsed_target_url.netloc == domain:
            return True
        else:
            return False

    def set_max_level(self, level):
        self.max_level = level

    def need_js_rendering(self, html):
        return '<script' in html.lower()

    def __del__(self):
        self.session.close()

    def __bool__(self):
        return True

class WebURLIterator(Iterator):
    def __init__(self, auth, resources={}, max_workers=8):
        self.max_workers = max_workers
        self.resources = resources
        self.logger = logging.getLogger(self.__class__.__name__)
        self.urls_pool = self.resources['children']
        self.current_item = self.urls_pool.pop()
        self.current_url = self.current_item['url']
        self.max_level = self.current_item['max_level']
        self.stack = list()
        self.touched = dict()
        self.url_list_stack = Queue()
        self.node_content = dict()
        self.is_root = True
        self.client = WebURL(self.max_level)
        self.ignore_ext = compile(r'\.(pdf|zip|docx?|xlsx?|mp3|mp4)(\?|$)', I)
        self.node = WebURLDict()
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            futures = []
            if len(self.stack) > 0:
                self.node = WebURLDict(self.stack.pop())
                self.node['cls'] = 'WEBURL'
                self.logger.info('level:{}, url:{}'.format(self.node['level'], self.node['path']))
                return self.node, partial(
                    self.get_file,
                    self.node
                )
            elif self.is_root:
                self.touched[self.current_url] = 0
                url_details, level = self.client.get_nodes(self.current_url, 0)
                self.is_root = False
                self.update_touched(url_details['sub_link_list'],level)
                # print(len(self.touched))
                self.details_to_stack(url_details, level)
            elif not self.url_list_stack.empty():
                # self.current_url = self.url_list_stack.get()
                try:
                    batch_size = min(self.max_workers, self.url_list_stack.qsize())
                    for _ in range(batch_size):
                        self.current_url = self.url_list_stack.get()
                        future = self.executor.submit(self.process_url, self.current_url, self.touched[self.current_url])
                        futures.append(future)
                    for future in as_completed(futures):
                        url_details, level = future.result()
                        self.update_touched(url_details['sub_link_list'], level)
                        self.details_to_stack(url_details, level)
                    # print(f"线程池活跃线程数：{len(self.executor._threads)}")
                except (OSError) as e:
                    self.logger.error('OSERROR')
            elif len(self.urls_pool) > 0:
                self.current_item = self.urls_pool.pop()
                self.current_url = self.current_item['url']
                self.max_level = self.current_item['max_level']
                self.client.set_max_level(self.max_level)
                self.is_root = True
            else:
                self.url_list_stack.task_done()
                raise StopIteration

    def get_file(self, node):
        content = self.node_content.pop(node['path'])
        f = io.BytesIO()
        f.write(content.encode('utf-8'))
        f.seek(0)
        md5 = hashlib.md5()
        md5.update(f.getbuffer())
        node['md5'] = md5.hexdigest()
        return f.getvalue()

    def update_touched(self, urls, level):
        filtered_url = self.filter_links(urls)
        for url in filtered_url:
            if url not in self.touched:
                self.touched[url]=level
                self.url_list_stack.put(url)

    def filter_links(self, urls):
        return [url for url in urls if not self.ignore_ext.search(url)]

    def details_to_stack(self, url_details,level):
        node = {
            'name':'{}.html'.format(uuid.uuid4().hex),
            'path':url_details['url'],
            'size':len(url_details['text_content']),
            'type':'file',
            'level': level
        }
        self.logger.info(node)
        self.stack.append(node)
        self.node_content[url_details['url']] = url_details['text_content']

    def process_url(self, url, level):
        url_details, new_level = self.client.multi_get_nodes(url, level)
        return url_details, new_level

# http://edu.shandong.gov.cn/
# http://dnr.shandong.gov.cn/
# http://edu.shandong.gov.cn/art/2025/4/1/art_11992_10338035.html
# http://edu.shandong.gov.cn//col/col11969/index.html
# http://edu.shandong.gov.cn/col/col11969/index.html
# 使用示例
# if __name__ == "__main__":
#     start_time = time.time()
#     test_item = WebURLIterator({}, {'children': [{'url': 'http://dnr.shandong.gov.cn/', 'max_level': 2}]})
#     # test_item = WebURLIterator({},{'max_level':2,'children':['http://edu.shandong.gov.cn/picture/0/92e9360e88d84622888f9d44d33ebfb8.jpg']})
#     num = 0
#     try:
#         while True:
#             node, get_file = next(test_item)
#             print('{}:level:{}, url:{}'.format(num,node['level'],node['path']))
#             f = get_file()
#             print(len(f))
#             num = num + 1
#     except StopIteration:
#         print('End')
#
#
#     print(f"Total time: {time.time() - start_time:.2f}s")


