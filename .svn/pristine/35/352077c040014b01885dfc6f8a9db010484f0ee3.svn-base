# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import hashlib
import hmac
import io
import time
import os.path
import logging
import requests
from functools import partial
from threading import Lock
from tempfile import SpooledTemporaryFile
from common.functools import gen_key
from collections import UserDict
from requests import PreparedRequest
from datetime import datetime, timezone
from requests.auth import AuthBase
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from oauthlib.oauth2 import BackendApplicationClient
from exceptions import WPSError
from crawling.interface import Iterator, Client


class WKDOCDict(UserDict):

    def __getitem__(self, item):
        if item == "timestamp":
            return self.data["last_write_time"]
        elif item == "diff":
            return gen_key(
                path=self.data.get("path"),
                last_write_time=self.data.get("last_write_time"),
            )
        else:
            return self.data[item]

    def __contains__(self, item):
        if item == "timestamp":
            return "last_write_time" in self.data
        elif item == "diff":
            return {"last_write_time", "path"}.issubset(self.data)
        else:
            return item in self.data


class WKDOCAuth(AuthBase):

    def __init__(
        self,
        endpoint,
        client_id: str,
        client_secret: str,
        scope: None | str | list[str] = None,
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.lock = Lock()
        self.endpoint = endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.client = BackendApplicationClient(client_id=self.client_id)
        self.token_url = os.path.join(self.endpoint, "oauth2/token")
        self.session = requests.Session()
        self.session.verify = False
        retries = Retry(
            total=3,
            backoff_factor=3,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.token = self.fetch_token()
        self.refresh_time = time.time()

    @property
    def token_type(self):
        return self.token.get("token_type")

    @property
    def access_token(self):
        return self.token.get("access_token")

    @property
    def expires_in(self):
        return self.token.get("expires_in")

    def fetch_token(self):
        body = self.client.prepare_request_body(
            include_client_id=True,
            client_id=self.client_id,
            client_secret=self.client_secret,
            scope=self.scope
        )
        resp = self.session.post(
            self.token_url,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        self.logger.debug('request:')
        self.logger.debug(resp.request.url)
        self.logger.debug(resp.request.headers)
        self.logger.debug('response:')
        self.logger.debug(resp.status_code)
        self.logger.debug(resp.headers)
        if resp.status_code >= 300:
            if resp.headers.get("Content-Type") == "application/json":
                json = resp.json()
                raise WPSError(json)
            else:
                self.logger.error(resp.text)
                resp.raise_for_status()
        else:
            return resp.json()

    def check_expires(self):
        if self.refresh_time + self.expires_in < time.time() + 60:
            return True
        else:
            return False

    def refresh_token(self):
        if self.lock.acquire(blocking=False):
            try:
                if self.check_expires():
                    self.token = self.fetch_token()
                    self.refresh_time = time.time()
            finally:
                self.lock.release()

    def __call__(self, r):
        self.refresh_token()
        kso_date = datetime.now(
            timezone.utc
        ).strftime(
            "%a, %d %b %Y %H:%M:%S GMT"
        )
        method = r.method.upper()
        request_uri = r.url.removeprefix(self.endpoint)
        content_type = r.headers.get("Content-Type", "")
        request_body = r.body
        text = io.StringIO()
        text.write("KSO-1")
        text.write(method)
        text.write(request_uri)
        text.write(content_type)
        text.write(kso_date)
        if request_body:
            if isinstance(request_body, str):
                request_body = request_body.encode()
            text.write(hashlib.sha256(request_body).hexdigest())
        signature = hmac.new(
            self.client_secret.encode(), text.getvalue().encode(), hashlib.sha256
        ).hexdigest()
        # r.headers['X-Kso-Id-Type'] = 'internal'
        r.headers["X-Kso-Date"] = kso_date
        r.headers["X-Kso-Authorization"] = f"KSO-1 {self.client_id}:{signature}"
        r.headers["Authorization"] = f"{self.token_type} {self.access_token}"
        return r


class WKDOC(Client):

    def __init__(self, endpoint, client_id, client_secret, scope=None):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.endpoint = endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.auth = WKDOCAuth(
            endpoint=self.endpoint,
            client_id=self.client_id,
            client_secret=self.client_secret,
            scope=self.scope,
        )

        self.session = requests.Session()
        self.session.verify = False
        self.session.hooks = {"response": self.refresh_token}
        retries = Retry(
            total=3, backoff_factor=3, status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def refresh_token(self, r, *args, **kwargs):
        if 300 <= r.status_code < 500:
            if r.headers.get("Content-Type") == "application/json":
                json = r.json()
                if int(json.get("code")) == 40100011:
                    self.auth.refresh_token()
        return r

    def get_drives(self, page_size=100, page_token=None):
        nodes = list()
        url = os.path.join(self.endpoint, "v7/drives/authorized")
        params = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        resp = self.session.get(url, params=params, auth=self.auth)
        self.logger.debug('request:')
        self.logger.debug(resp.request.url)
        self.logger.debug(resp.request.headers)
        self.logger.debug('response:')
        self.logger.debug(resp.status_code)
        self.logger.debug(resp.headers)
        if resp.status_code >= 300:
            if resp.headers.get("Content-Type") == "application/json":
                json = resp.json()
                raise WPSError(json)
            else:
                self.logger.debug(resp.text)
                resp.raise_for_status()
        else:
            json = resp.json()
            if json.get("code") == 0:
                for item in json.get("data").get("items"):
                    node = dict()
                    node["type"] = "dir"
                    node["name"] = item["name"]
                    node["path"] = item["name"]
                    node["allotee_id"] = item["allotee_id"]
                    node["allotee_type"] = item["allotee_type"]
                    node["company_id"] = item["company_id"]
                    node["drive_created_by"] = item["created_by"]
                    node["create_time"] = item["ctime"]
                    node["description"] = item["description"]
                    node["last_write_time"] = item["mtime"]
                    node["drive_id"] = item["id"]
                    node["quota"] = item["quota"]
                    node["status"] = item["status"]
                    node["children"] = []
                    nodes.append(node)
                else:
                    next_page_token = json.get("data").get("next_page_token")
                    if next_page_token:
                        node = dict()
                        node["type"] = "drive"
                        node["name"] = "..."
                        node["page_token"] = next_page_token
                        node["sibling"] = []
                        nodes.append(node)
            else:
                raise WPSError(json)
        return nodes

    def listdir(self, drive_id, parent_id, page_size=100, page_token=None):
        nodes = list()
        url = os.path.join(
            self.endpoint,
            f"v7/drives/{drive_id}/files/{parent_id}/children"
        )
        params = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token

        resp = self.session.get(url, params=params, auth=self.auth)
        self.logger.debug('request:')
        self.logger.debug(resp.request.url)
        self.logger.debug(resp.request.headers)
        self.logger.debug('response:')
        self.logger.debug(resp.status_code)
        self.logger.debug(resp.headers)
        if resp.status_code >= 300:
            if resp.headers.get("Content-Type") == "application/json":
                json = resp.json()
                raise WPSError(json)
            else:
                self.logger.debug(resp.text)
                resp.raise_for_status()
        else:
            json = resp.json()
            if json.get("code") == 0:
                for item in json.get("data").get("items"):
                    node = dict()
                    if item["type"] == "file":
                        node["type"] = "file"
                    elif item["type"] == "folder":
                        node["type"] = "dir"
                        node["children"] = []
                    else:
                        self.logger.info(item)
                        continue
                    node["created_by"] = item["created_by"]
                    node["create_time"] = item["ctime"]
                    node["drive_id"] = item["drive_id"]
                    node["file_id"] = item["id"]
                    node["modified_by"] = item["modified_by"]
                    node["last_write_time"] = item["mtime"]
                    node["name"] = item["name"]
                    node["parent_id"] = item["parent_id"]
                    node["size"] = item["size"]
                    node["version"] = item["version"]
                    node['link_id'] = item.get('link_id', '')
                    node['link_url'] = item.get('link_url', '')
                    nodes.append(node)
                else:
                    next_page_token = json.get("data").get("next_page_token")
                    if next_page_token:
                        node = dict()
                        node["type"] = "dir"
                        node["name"] = "..."
                        node["drive_id"] = drive_id
                        node["parent_id"] = parent_id
                        node["page_token"] = next_page_token
                        node["sibling"] = []
                        nodes.append(node)
            else:
                raise WPSError(json)
        return nodes

    def get_download_url(self, drive_id, file_id):
        url = os.path.join(
            self.endpoint,
            f"v7/drives/{drive_id}/files/{file_id}/download"
        )
        resp = self.session.get(url, auth=self.auth)
        self.logger.debug('request:')
        self.logger.debug(resp.request.url)
        self.logger.debug(resp.request.headers)
        self.logger.debug('response:')
        self.logger.debug(resp.status_code)
        self.logger.debug(resp.headers)
        if resp.status_code >= 300:
            if resp.headers.get("Content-Type") == "application/json":
                json = resp.json()
                raise WPSError(json)
            else:
                self.logger.debug(resp.text)  
                resp.raise_for_status()
        else:
            json = resp.json()
            return json.get("data").get("url")

    def get_file(self, node):
        drive_id = node["drive_id"]
        file_id = node["file_id"]
        url = self.get_download_url(drive_id, file_id)
        f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
        resp = self.session.get(url, stream=True)
        self.logger.debug('request:')
        self.logger.debug(resp.request.url)
        self.logger.debug(resp.request.headers)
        self.logger.debug('response:')
        self.logger.debug(resp.status_code)
        self.logger.debug(resp.headers)
        if resp.status_code >= 300:
            self.logger.debug(resp.text)
            resp.raise_for_status()
        else:
            for chunk in resp.iter_content(chunk_size=2**18):
                f.write(chunk)
            f.seek(0)
        return f

    def get_nodes(self, node):
        if node.get("type", "drive") == "drive":
            params = {"page_token": node.get("page_token")}
            drives = self.get_drives(**params)
            return drives
        else:
            params = {
                "drive_id": node["drive_id"],
                "parent_id": node.get("file_id", 0),
                "page_token": node.get("page_token"),
            }
            files = self.listdir(**params)
            for file in files:
                file["path"] = os.path.join(node["path"], file["name"])
                file["allotee_id"] = node["allotee_id"]
                file["allotee_type"] = node["allotee_type"]
                file['drive_created_by'] = node['drive_created_by']
            return files

    def __bool__(self):
        return True

    def __del__(self):
        self.session.close()


class WKDOCIterator(Iterator):

    def __init__(self, auth, resources={}):

        self.auth = auth
        self.client = WKDOC(**self.auth)
        self.stack = list()
        if resources:
            self.stack.append(resources)
        self.node = dict()

    def __iter__(self):
        return self

    def __bool__(self):
        return bool(self.stack)

    def __next__(self):
        while len(self.stack) > 0:
            self.node = WKDOCDict(self.stack.pop())
            self.node["auth"] = self.auth
            self.node["cls"] = "WKDOC"
            if "children" in self.node:
                if self.node["children"]:
                    children = self.node["children"]
                else:
                    children = self.get_nodes(self.node)
                children.reverse()
                self.stack.extend(children)
            elif "sibling" in self.node:
                if self.node["sibling"]:
                    sibling = self.node["sibling"]
                else:
                    sibling = self.get_nodes(self.node)
                sibling.reverse()
                self.stack.extend(sibling)
            else:
                return self.node, partial(self.get_file, self.node)
        raise StopIteration

    def get_nodes(self, node):
        nodes = self.client.get_nodes(node)
        return nodes

    def add_nodes(self, nodes):
        self.stack.extend(nodes)

    def get_file(self, node):
        f = self.client.get_file(node)
        md5 = hashlib.file_digest(f, "md5")
        node["md5"] = md5.hexdigest()
        f.seek(0)
        return f
