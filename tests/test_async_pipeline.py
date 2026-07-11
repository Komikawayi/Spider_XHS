from types import SimpleNamespace
import asyncio
import unittest
from unittest.mock import patch

from scripts.spider_hangzhou_ecommerce import CookieAccount, CookieAccountPool
from scripts.spider_hangzhou_ecommerce_async import AsyncAccountPool, run_workers
from xhs_utils.crawl_metrics import CrawlMetrics


class FakeAsyncApi:
    def __init__(self):
        self.active = 0
        self.max_active = 0

    async def get_note_info(self, url, cookies):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.01)
        self.active -= 1
        note_id = url.rsplit("/", 1)[-1].split("?", 1)[0]
        return True, "成功", {"data": {"items": [{"id": note_id}]}}


class AsyncPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_respects_per_account_capacity_with_more_async_workers(self):
        account = CookieAccount("account_1", "a1=test", max_concurrency=1)
        pool = AsyncAccountPool(CookieAccountPool([account]))
        api = FakeAsyncApi()
        args = SimpleNamespace(note_concurrency=10, max_comments_per_note=0, query="测试")
        notes = [
            {"id": "note-1", "model_type": "note", "note_card": {"type": "normal"}},
            {"id": "note-2", "model_type": "note", "note_card": {"type": "normal"}},
        ]

        with patch(
            "scripts.spider_hangzhou_ecommerce_async.handle_note_info",
            side_effect=lambda raw: {"note_id": raw["id"], "note_type": "图集", "title": raw["id"]},
        ):
            results = await run_workers(api, pool, notes, args, None, CrawlMetrics())

        self.assertEqual(len(results), 2)
        self.assertEqual(api.max_active, 1)
        self.assertEqual(account.active_count, 0)

