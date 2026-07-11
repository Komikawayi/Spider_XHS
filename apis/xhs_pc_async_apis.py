# encoding: utf-8
"""aiohttp 版小红书 PC 端核心读取接口。"""

import asyncio
import json
import time
import urllib.parse

import aiohttp
from loguru import logger

from xhs_utils.http_util import REQUEST_TIMEOUT
from xhs_utils.xhs_util import (
    generate_request_params,
    generate_search_id,
    generate_x_rap_param,
    splice_str,
)


def _success_message(response):
    success = response.get("success", False)
    return success, response.get("msg", "成功" if success else str(response))


def _url_query(url):
    return {
        key: values[-1] if values else ""
        for key, values in urllib.parse.parse_qs(
            urllib.parse.urlparse(url).query, keep_blank_values=True
        ).items()
    }


class AsyncXHSApi:
    """复用 aiohttp 连接、显式携带每次请求的账号 Cookie。"""

    def __init__(self, metrics=None, signer=None, connector_limit=10, session_factory=None):
        self.base_url = "https://edith.xiaohongshu.com"
        self._metrics = metrics
        self._signer = signer
        self._connector_limit = connector_limit
        self._session_factory = session_factory
        self._session = None

    async def _get_session(self):
        if self._session is not None:
            return self._session
        if self._session_factory:
            self._session = self._session_factory()
        else:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(
                    limit=self._connector_limit,
                    limit_per_host=self._connector_limit,
                ),
                cookie_jar=aiohttp.DummyCookieJar(),
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            )
        if self._metrics:
            self._metrics.increment("http.sessions_created")
        return self._session

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _request_json(self, method, api, operation, **kwargs):
        session = await self._get_session()
        started_at = time.monotonic()
        metric_name = "http." + operation.strip("/").replace("/", ".")
        try:
            async with session.request(method, self.base_url + api, **kwargs) as response:
                payload = await response.json(content_type=None)
                ok = 200 <= response.status < 400
        except Exception:
            if self._metrics:
                self._metrics.record_duration(
                    metric_name, time.monotonic() - started_at, success=False
                )
            raise
        if self._metrics:
            self._metrics.record_duration(
                metric_name, time.monotonic() - started_at, success=ok
            )
        return payload

    async def _request_params(self, cookies_str, api, data="", method="POST"):
        generator = (
            self._signer.generate_request_params
            if self._signer else generate_request_params
        )
        return await asyncio.to_thread(generator, cookies_str, api, data, method)

    async def _x_rap_param(self, api, data):
        generator = self._signer.generate_x_rap_param if self._signer else generate_x_rap_param
        return await asyncio.to_thread(generator, api, data)

    @staticmethod
    def _proxy(proxies):
        return (proxies or {}).get("https") or (proxies or {}).get("http")

    async def search_note(
        self, query, cookies_str, page=1, sort_type_choice=0, note_type=0,
        note_time=0, note_range=0, pos_distance=0, geo="", search_id=None,
        proxies=None,
    ):
        sort_type = {
            1: "time_descending", 2: "popularity_descending",
            3: "comment_descending", 4: "collect_descending",
        }.get(sort_type_choice, "general")
        filter_note_type = {1: "视频笔记", 2: "普通笔记"}.get(note_type, "不限")
        filter_note_time = {1: "一天内", 2: "一周内", 3: "半年内"}.get(note_time, "不限")
        filter_note_range = {1: "已看过", 2: "未看过", 3: "已关注"}.get(note_range, "不限")
        filter_pos_distance = {1: "同城", 2: "附近"}.get(pos_distance, "不限")
        if geo:
            geo = json.dumps(geo, separators=(",", ":"))
        api = "/api/sns/web/v1/search/notes"
        payload = {
            "keyword": query,
            "page": page,
            "page_size": 20,
            "search_id": search_id or generate_search_id(),
            "sort": "general",
            "note_type": 0,
            "ext_flags": [],
            "filters": [
                {"tags": [sort_type], "type": "sort_type"},
                {"tags": [filter_note_type], "type": "filter_note_type"},
                {"tags": [filter_note_time], "type": "filter_note_time"},
                {"tags": [filter_note_range], "type": "filter_note_range"},
                {"tags": [filter_pos_distance], "type": "filter_pos_distance"},
            ],
            "geo": geo,
            "image_formats": ["jpg", "webp", "avif"],
        }
        try:
            headers, cookies, body = await self._request_params(cookies_str, api, payload, "POST")
            headers["x-rap-param"] = await self._x_rap_param(api, body)
            response = await self._request_json(
                "POST", api, api, headers=headers, data=body.encode("utf-8"),
                cookies=cookies, proxy=self._proxy(proxies),
            )
            success, message = _success_message(response)
        except Exception as error:
            logger.exception(f"XHS async search request failed: {error}")
            return False, str(error), None
        return success, message, response

    async def get_note_info(self, url, cookies_str, proxies=None):
        try:
            parsed = urllib.parse.urlparse(url)
            note_id = parsed.path.split("/")[-1]
            query = _url_query(url)
            api = "/api/sns/web/v1/feed"
            payload = {
                "source_note_id": note_id,
                "image_formats": ["jpg", "webp", "avif"],
                "extra": {"need_body_topic": "1"},
                "xsec_source": query.get("xsec_source", "pc_search"),
                "xsec_token": query.get("xsec_token", ""),
            }
            headers, cookies, body = await self._request_params(cookies_str, api, payload, "POST")
            headers["x-rap-param"] = await self._x_rap_param(api, body)
            headers["xy-direction"] = "13"
            response = await self._request_json(
                "POST", api, api, headers=headers, data=body, cookies=cookies,
                proxy=self._proxy(proxies),
            )
            success, message = _success_message(response)
        except Exception as error:
            logger.exception(f"XHS async note request failed: {error}")
            return False, str(error), None
        return success, message, response

    async def get_note_out_comment(self, note_id, cursor, xsec_token, cookies_str, proxies=None):
        api = "/api/sns/web/v2/comment/page"
        params = {
            "note_id": note_id,
            "cursor": cursor,
            "top_comment_id": "",
            "image_formats": "jpg,webp,avif",
            "xsec_token": xsec_token,
        }
        signed_api = splice_str(api, params)
        try:
            headers, cookies, _ = await self._request_params(cookies_str, signed_api, "", "GET")
            response = await self._request_json(
                "GET", signed_api, api, headers=headers, cookies=cookies,
                proxy=self._proxy(proxies),
            )
            success, message = _success_message(response)
        except Exception as error:
            logger.exception(f"XHS async comment request failed: {error}")
            return False, str(error), None
        return success, message, response
