from types import SimpleNamespace
import asyncio
import unittest
from unittest.mock import patch

from scripts.spider_hangzhou_ecommerce import CookieAccount, CookieAccountPool
from scripts.spider_hangzhou_ecommerce_async import (
    AsyncAccountPool,
    PipelineState,
    build_parser,
    run_workers,
    search_producer,
    validate_args,
)
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


class FakePagedSearchApi:
    def __init__(self):
        self.search_ids = []

    async def search_note(self, query, cookies, page=1, search_id=None, **kwargs):
        self.search_ids.append(search_id)
        items = [
            {"id": f"note-{page}", "model_type": "note", "note_card": {"type": "normal"}}
        ]
        return True, "成功", {"data": {"items": items, "has_more": page < 2}}


class FakeCommentApi(FakeAsyncApi):
    def __init__(self):
        super().__init__()
        self.comment_pages = 0

    async def get_note_out_comment(self, note_id, cursor, xsec_token, cookies):
        self.comment_pages += 1
        return True, "成功", {
            "data": {
                "comments": [{"id": f"comment-{self.comment_pages}"}],
                "cursor": str(self.comment_pages),
                "has_more": self.comment_pages < 2,
            }
        }


class FakeUnavailableApi:
    async def get_note_info(self, url, cookies):
        return True, "成功", {"data": {}}


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

    async def test_search_producer_reuses_root_search_id_across_pages(self):
        account = CookieAccount("account_1", "a1=test", max_concurrency=1)
        pool = AsyncAccountPool(CookieAccountPool([account]))
        api = FakePagedSearchApi()
        args = SimpleNamespace(
            query="测试", max_search_pages=5, sort_type_choice=0,
            note_time=0, note_range=0, max_comments_per_note=10,
            ignore_existing_progress=False,
        )
        queue = asyncio.Queue(maxsize=10)
        state = PipelineState(target_notes=2, target_comments=20)
        metrics = CrawlMetrics()

        await search_producer(api, pool, args, queue, state, metrics)

        self.assertEqual(queue.qsize(), 2)
        self.assertEqual(len(set(api.search_ids)), 1)
        self.assertEqual(metrics.snapshot()["counters"]["search.pages"], 2)

    async def test_missing_detail_items_is_counted_and_skipped(self):
        account = CookieAccount("account_1", "a1=test", max_concurrency=1)
        pool = AsyncAccountPool(CookieAccountPool([account]))
        args = SimpleNamespace(
            note_concurrency=1, comment_concurrency=1, require_num=1,
            target_comments=10, max_comments_per_note=0, query="测试",
        )
        metrics = CrawlMetrics()
        notes = [{"id": "missing", "model_type": "note", "note_card": {"type": "normal"}}]

        results = await run_workers(FakeUnavailableApi(), pool, notes, args, None, metrics)

        self.assertEqual(results, [])
        self.assertEqual(metrics.snapshot()["counters"]["notes.unavailable"], 1)
        self.assertEqual(account.active_count, 0)

    async def test_comment_page_is_requeued_until_has_more_is_false(self):
        account = CookieAccount("account_1", "a1=test", max_concurrency=1)
        pool = AsyncAccountPool(CookieAccountPool([account]))
        args = SimpleNamespace(
            note_concurrency=1, comment_concurrency=1, require_num=1,
            target_comments=10, max_comments_per_note=10, query="测试",
        )
        api = FakeCommentApi()
        metrics = CrawlMetrics()
        notes = [{"id": "note-1", "model_type": "note", "note_card": {"type": "normal"}}]

        with patch(
            "scripts.spider_hangzhou_ecommerce_async.handle_note_info",
            return_value={"note_id": "note-1", "note_type": "图集", "title": "测试"},
        ), patch(
            "scripts.spider_hangzhou_ecommerce_async.parse_comment",
            side_effect=lambda raw, note_id, title: {"comment_id": raw["id"]},
        ):
            results = await run_workers(api, pool, notes, args, None, metrics)

        snapshot = metrics.snapshot()["counters"]
        self.assertEqual(len(results), 1)
        self.assertEqual(api.comment_pages, 2)
        self.assertEqual(snapshot["comments.requeued"], 1)
        self.assertEqual(snapshot["comments.saved"], 2)

    def test_calibration_mode_locks_account_and_forces_dry_run(self):
        args = build_parser().parse_args([
            "--calibration-mode", "--account-name", "account_10",
            "--fixed-concurrency", "8",
        ])
        validate_args(args)
        self.assertTrue(args.dry_run)
        self.assertTrue(args.ignore_existing_progress)
        self.assertFalse(args.use_postgres)
