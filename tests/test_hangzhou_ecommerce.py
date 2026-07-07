import json
import tempfile
import threading
import time
import unittest
from datetime import datetime
from pathlib import Path

from scripts import spider_hangzhou_ecommerce as hz


class FakeCommentApi:
    def __init__(self):
        self.calls = 0

    def get_note_out_comment(self, note_id, cursor, xsec_token, cookies_str, proxies=None):
        self.calls += 1
        comments = [
            {"id": f"c{self.calls}a", "note_id": note_id, "content": "a", "sub_comments": [], "sub_comment_has_more": False},
            {"id": f"c{self.calls}b", "note_id": note_id, "content": "b", "sub_comments": [], "sub_comment_has_more": False},
        ]
        return True, "成功", {"data": {"comments": comments, "cursor": str(self.calls), "has_more": self.calls < 3}}


class ExpiredCommentApi:
    def get_note_out_comment(self, note_id, cursor, xsec_token, cookies_str, proxies=None):
        return False, "登录已过期", {}


class EmptyThenSuccessSearchApi:
    def __init__(self):
        self.calls = 0

    def search_some_note(self, *args, **kwargs):
        self.calls += 1
        if self.calls == 1:
            return True, "成功", []
        return True, "成功", [{"id": "n1", "model_type": "note", "note_card": {"type": "normal"}}]


class AlwaysEmptySearchApi:
    def search_some_note(self, *args, **kwargs):
        return True, "成功", []


class SlowNoteApi:
    def __init__(self):
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()

    def get_note_info(self, note_url, cookies_str):
        note_id = note_url.rsplit('/', 1)[-1].split('?', 1)[0]
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        time.sleep(0.05)
        with self.lock:
            self.active -= 1
        return True, "成功", {
            "data": {
                "items": [
                    {
                        "id": note_id,
                        "note_card": {
                            "type": "normal",
                            "title": f"title-{note_id}",
                            "desc": "",
                            "user": {"nickname": "u", "user_id": "uid", "avatar": "avatar"},
                            "interact_info": {
                                "liked_count": 0,
                                "collected_count": 0,
                                "comment_count": 0,
                                "share_count": 0,
                            },
                            "image_list": [],
                            "tag_list": [],
                            "time": 0,
                            "last_update_time": 0,
                            "ip_location": "",
                        },
                    }
                ]
            }
        }

    def get_note_out_comment(self, note_id, cursor, xsec_token, cookies_str, proxies=None):
        return True, "成功", {"data": {"comments": [], "has_more": False}}


class VariableSpeedNoteApi(SlowNoteApi):
    def get_note_info(self, note_url, cookies_str):
        note_id = note_url.rsplit('/', 1)[-1].split('?', 1)[0]
        if note_id == "slow":
            time.sleep(0.25)
        return super().get_note_info(note_url, cookies_str)


class HasMoreAtLimitApi(SlowNoteApi):
    def get_note_out_comment(self, note_id, cursor, xsec_token, cookies_str, proxies=None):
        return True, "成功", {
            "data": {
                "comments": [
                    {"id": "c1", "content": "first", "user_info": {}, "like_count": 0},
                    {"id": "c2", "content": "second", "user_info": {}, "like_count": 0},
                ],
                "cursor": "cursor-2",
                "has_more": True,
            }
        }


class ResumeCommentApi(SlowNoteApi):
    def __init__(self):
        super().__init__()
        self.comment_cursors = []

    def get_note_out_comment(self, note_id, cursor, xsec_token, cookies_str, proxies=None):
        self.comment_cursors.append(cursor)
        if cursor == "resume-cursor":
            comments = [
                {"id": "c3", "content": "third", "user_info": {}, "like_count": 0},
                {"id": "c4", "content": "fourth", "user_info": {}, "like_count": 0},
            ]
            return True, "成功", {"data": {"comments": comments, "cursor": "", "has_more": False}}
        comments = [
            {"id": "c1", "content": "first", "user_info": {}, "like_count": 0},
            {"id": "c2", "content": "second", "user_info": {}, "like_count": 0},
        ]
        return True, "成功", {"data": {"comments": comments, "cursor": "resume-cursor", "has_more": True}}


class MissingItemsNoteApi(SlowNoteApi):
    def get_note_info(self, note_url, cookies_str):
        return True, "成功", {"data": {}}


class FakeProgressStorage:
    def __init__(self, progress=None):
        self.progress = progress or {}
        self.notes = []
        self.comments = []

    def get_comment_progress_by_note_ids(self, note_ids):
        return {note_id: self.progress[note_id] for note_id in note_ids if note_id in self.progress}

    def upsert_note(self, note, keyword="", raw=None):
        self.notes.append(note)

    def upsert_comment(self, comment, note_id, note_title="", parent_comment_id=None, raw=None):
        self.comments.append(comment)

    def count_comments(self, note_id):
        return len({comment["comment_id"] for comment in self.comments})

    def update_comment_progress(self, note_id, status, collected_count, target_count, next_cursor, has_more):
        self.progress[note_id] = {
            "comments_crawl_status": status,
            "comments_collected_count": collected_count,
            "comments_target_count": target_count,
            "comments_next_cursor": next_cursor,
            "comments_has_more": has_more,
        }


class HangzhouEcommerceScriptTests(unittest.TestCase):
    def test_iter_image_notes_keeps_only_normal_note_cards(self):
        notes = [
            {"id": "normal-1", "model_type": "note", "note_card": {"type": "normal"}},
            {"id": "video-1", "model_type": "note", "note_card": {"type": "video"}},
            {"id": "user-1", "model_type": "user", "note_card": {"type": "normal"}},
            {"id": "normal-2", "model_type": "note", "note_card": {}},
        ]

        kept = list(hz.iter_image_notes(notes))

        self.assertEqual([n["id"] for n in kept], ["normal-1", "normal-2"])

    def test_collect_comments_limited_stops_before_fetching_extra_pages(self):
        api = FakeCommentApi()
        note_url = "https://www.xiaohongshu.com/explore/note123?xsec_token=tok123"

        comments = hz.collect_comments_limited(api, note_url, "cookie", max_comments=1, delay_seconds=0)

        self.assertEqual(len(comments), 1)
        self.assertEqual(api.calls, 1)

    def test_build_note_url_includes_xsec_token_when_available(self):
        url = hz.build_note_url({"id": "abc", "xsec_token": "token=="})

        self.assertEqual(url, "https://www.xiaohongshu.com/explore/abc?xsec_token=token%3D%3D")

    def test_should_stop_for_message_detects_risk_or_login_messages(self):
        self.assertTrue(hz.should_stop_for_message("登录已过期"))
        self.assertTrue(hz.should_stop_for_message("请求过于频繁"))
        self.assertFalse(hz.should_stop_for_message("成功"))

    def test_collect_comments_limited_raises_stop_crawl_on_login_expired(self):
        note_url = "https://www.xiaohongshu.com/explore/note123?xsec_token=tok123"

        with self.assertRaises(hz.StopCrawl):
            hz.collect_comments_limited(ExpiredCommentApi(), note_url, "cookie", max_comments=1, delay_seconds=0)

    def test_default_args_save_to_postgres_and_excel(self):
        args = hz.build_parser().parse_args([])

        self.assertEqual(args.query, hz.DEFAULT_QUERY)
        self.assertEqual(args.require_num, hz.DEFAULT_REQUIRE_NUM)
        self.assertEqual(args.max_comments_per_note, hz.DEFAULT_MAX_COMMENTS_PER_NOTE)
        self.assertEqual(args.delay_seconds, hz.DEFAULT_DELAY_SECONDS)
        self.assertEqual(args.note_concurrency, hz.DEFAULT_NOTE_CONCURRENCY)
        self.assertTrue(args.use_postgres)
        self.assertTrue(args.save_excel)
        self.assertFalse(args.dry_run)
        hz.validate_args(args)

    def test_validate_args_allows_explicit_dry_run(self):
        args = hz.build_parser().parse_args(["--dry-run"])

        hz.validate_args(args)

    def test_dry_run_disables_default_storage(self):
        args = hz.build_parser().parse_args(["--dry-run"])
        hz.validate_args(args)

        self.assertFalse(args.use_postgres)
        self.assertFalse(args.save_excel)

    def test_build_task_output_dir_uses_keyword_and_timestamp(self):
        base_path = {"excel": str(Path("datas") / "excel_datas")}
        started_at = datetime(2026, 7, 7, 16, 30, 5)

        output_dir = hz.build_task_output_dir(base_path, "杭州电商", started_at)

        self.assertEqual(
            output_dir,
            str(Path("datas") / "excel_datas" / "杭州电商_20260707_163005"),
        )

    def test_search_notes_with_retry_retries_empty_results(self):
        api = EmptyThenSuccessSearchApi()

        success, msg, notes = hz.search_notes_with_retry(
            api, "植村秀", 20, "cookie", 0, 2, 0, 0, retries=2, retry_delay_seconds=0
        )

        self.assertTrue(success)
        self.assertEqual(len(notes), 1)
        self.assertEqual(api.calls, 2)

    def test_search_notes_with_retry_fails_after_empty_retries(self):
        with self.assertRaises(RuntimeError):
            hz.search_notes_with_retry(
                AlwaysEmptySearchApi(), "植村秀", 20, "cookie", 0, 2, 0, 0, retries=1, retry_delay_seconds=0
            )

    def test_collect_note_results_yields_finished_notes_incrementally(self):
        api = VariableSpeedNoteApi()
        notes = [
            {"id": "fast", "model_type": "note", "note_card": {"type": "normal"}},
            {"id": "slow", "model_type": "note", "note_card": {"type": "normal"}},
        ]

        started = time.monotonic()
        results = hz.collect_note_results(
            notes,
            api,
            cookies_str="cookie",
            max_comments_per_note=0,
            delay_seconds=0,
            base_path={"media": "media"},
            download_media_flag=False,
            note_concurrency=2,
        )
        first = next(iter(results))
        elapsed = time.monotonic() - started

        self.assertIsNotNone(first)
        self.assertLess(elapsed, 0.2)


    def test_collect_notes_uses_note_concurrency(self):
        api = SlowNoteApi()
        notes = [
            {"id": f"n{i}", "model_type": "note", "note_card": {"type": "normal"}}
            for i in range(4)
        ]

        note_list, all_comments = hz.collect_notes(
            notes,
            api,
            cookies_str=hz.CookieAccountPool([
                hz.CookieAccount("a1", "cookie-1"),
                hz.CookieAccount("a2", "cookie-2"),
                hz.CookieAccount("a3", "cookie-3"),
            ]),
            max_comments_per_note=0,
            delay_seconds=0,
            base_path={"media": "media"},
            download_media_flag=False,
            note_concurrency=3,
        )

        self.assertEqual(len(note_list), 4)
        self.assertEqual(all_comments, [])
        self.assertGreater(api.max_active, 1)

    def test_load_cookie_accounts_from_json_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cookies.local.json"
            path.write_text(json.dumps([
                {"name": "a1", "cookies": "cookie-1"},
                {"name": "a2", "cookies": "cookie-2"},
            ]), encoding="utf-8")

            accounts = hz.load_cookie_accounts(str(path), fallback_cookie="fallback")

        self.assertEqual([account.name for account in accounts], ["a1", "a2"])
        self.assertEqual([account.cookies for account in accounts], ["cookie-1", "cookie-2"])

    def test_load_cookie_accounts_skips_inactive_json_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cookies.local.json"
            path.write_text(json.dumps([
                {"name": "active", "cookies": "cookie-1", "status": "active"},
                {"name": "legacy", "cookies": "cookie-2"},
                {"name": "expired", "cookies": "cookie-3", "status": "expired"},
                {"name": "disabled", "cookies": "cookie-4", "status": "disabled"},
            ]), encoding="utf-8")

            accounts = hz.load_cookie_accounts(str(path), fallback_cookie="fallback")

        self.assertEqual([account.name for account in accounts], ["active", "legacy"])
        self.assertEqual([account.cookies for account in accounts], ["cookie-1", "cookie-2"])

    def test_load_cookie_accounts_keeps_daily_usage_and_resets_old_day(self):
        today = hz.today_usage_date()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cookies.local.json"
            path.write_text(json.dumps([
                {
                    "name": "today",
                    "cookies": "cookie-1",
                    "daily_note_limit": 2,
                    "daily_comment_limit": 5,
                    "usage_date": today,
                    "note_ids_today": ["n1"],
                    "comments_today": 3,
                },
                {
                    "name": "old",
                    "cookies": "cookie-2",
                    "usage_date": "2000-01-01",
                    "note_ids_today": ["old-note"],
                    "comments_today": 99,
                },
            ]), encoding="utf-8")

            accounts = hz.load_cookie_accounts(str(path), fallback_cookie="fallback")

        self.assertEqual(accounts[0].daily_note_limit, 2)
        self.assertEqual(accounts[0].daily_comment_limit, 5)
        self.assertEqual(accounts[0].note_ids_today, {"n1"})
        self.assertEqual(accounts[0].comments_today, 3)
        self.assertEqual(accounts[1].usage_date, today)
        self.assertEqual(accounts[1].note_ids_today, set())
        self.assertEqual(accounts[1].comments_today, 0)

    def test_load_cookie_accounts_falls_back_to_env_cookie(self):
        accounts = hz.load_cookie_accounts(None, fallback_cookie="single-cookie")

        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[0].name, "default")
        self.assertEqual(accounts[0].cookies, "single-cookie")

    def test_cookie_pool_marks_expired_and_cooling_accounts_unavailable(self):
        pool = hz.CookieAccountPool([
            hz.CookieAccount("expired", "cookie-1"),
            hz.CookieAccount("limited", "cookie-2"),
        ])

        first = pool.acquire()
        pool.report_failure(first, "登录已过期")
        second = pool.acquire()
        pool.report_failure(second, "请求过于频繁")

        with self.assertRaises(hz.NoAvailableCookieAccounts):
            pool.acquire()
        self.assertEqual(first.status, "expired")
        self.assertEqual(second.status, "cooling")

    def test_cookie_pool_skips_accounts_that_reach_daily_note_limit(self):
        pool = hz.CookieAccountPool([
            hz.CookieAccount("a1", "cookie-1", daily_note_limit=1),
            hz.CookieAccount("a2", "cookie-2", daily_note_limit=1),
        ])

        first = pool.acquire_for_note("n1")
        pool.report_note_success(first, "n1")
        second = pool.acquire_for_note("n2")

        self.assertEqual(first.name, "a1")
        self.assertEqual(second.name, "a2")
        pool.report_success(second)

    def test_cookie_pool_does_not_count_same_note_twice(self):
        account = hz.CookieAccount("a1", "cookie-1", daily_note_limit=1)
        pool = hz.CookieAccountPool([account])

        first = pool.acquire_for_note("n1")
        pool.report_note_success(first, "n1")
        second = pool.acquire_for_note("n1")
        pool.report_note_success(second, "n1")

        self.assertEqual(account.note_ids_today, {"n1"})

    def test_cookie_pool_skips_accounts_that_reach_daily_comment_limit(self):
        pool = hz.CookieAccountPool([
            hz.CookieAccount("a1", "cookie-1", daily_comment_limit=2, comments_today=2),
            hz.CookieAccount("a2", "cookie-2", daily_comment_limit=2),
        ])

        account = pool.acquire_for_comments()
        pool.report_comments_success(account, 1)

        self.assertEqual(account.name, "a2")
        self.assertEqual(account.comments_today, 1)

    def test_filter_notes_skips_done_notes_when_platform_has_no_more_comments(self):
        notes = [
            {"id": "no-more", "model_type": "note", "note_card": {"type": "normal"}},
            {"id": "target-only", "model_type": "note", "note_card": {"type": "normal"}},
        ]
        storage = FakeProgressStorage({
            "no-more": {
                "comments_crawl_status": "done",
                "comments_target_count": 500,
                "comments_has_more": False,
            },
            "target-only": {
                "comments_crawl_status": "done",
                "comments_target_count": 500,
                "comments_has_more": True,
                "comments_next_cursor": "cursor-500",
            },
        })

        filtered = hz.filter_notes_for_collection(notes, storage, max_comments_per_note=1000)

        self.assertEqual([note["id"] for note in filtered], ["target-only"])
        self.assertEqual(filtered[0]["_comments_next_cursor"], "cursor-500")


    def test_filter_notes_skips_done_notes_and_keeps_partial_notes(self):
        notes = [
            {"id": "done", "model_type": "note", "note_card": {"type": "normal"}},
            {"id": "partial", "model_type": "note", "note_card": {"type": "normal"}},
            {"id": "new", "model_type": "note", "note_card": {"type": "normal"}},
        ]
        storage = FakeProgressStorage({
            "done": {"comments_crawl_status": "done", "comments_target_count": 500},
            "partial": {
                "comments_crawl_status": "partial",
                "comments_target_count": 500,
                "comments_next_cursor": "resume-cursor",
                "comments_collected_count": 200,
            },
        })

        filtered = hz.filter_notes_for_collection(notes, storage, max_comments_per_note=500)

        self.assertEqual([note["id"] for note in filtered], ["partial", "new"])
        self.assertEqual(filtered[0]["_comments_next_cursor"], "resume-cursor")

    def test_collect_note_result_preserves_has_more_when_target_limit_is_reached(self):
        api = HasMoreAtLimitApi()
        storage = FakeProgressStorage()
        pool = hz.CookieAccountPool([hz.CookieAccount("a1", "cookie-1")])
        note = {"id": "n-limit", "model_type": "note", "note_card": {"type": "normal"}}

        hz.collect_note_result(
            0, 1, note, api, pool,
            max_comments_per_note=2,
            delay_seconds=0,
            base_path={"media": "media"},
            download_media_flag=False,
            storage=storage,
            keyword="植村秀",
            storage_lock=threading.Lock(),
        )

        progress = storage.progress["n-limit"]
        self.assertEqual(progress["comments_crawl_status"], "done")
        self.assertTrue(progress["comments_has_more"])
        self.assertEqual(progress["comments_next_cursor"], "cursor-2")


    def test_collect_note_result_resumes_comments_from_saved_cursor(self):
        api = ResumeCommentApi()
        storage = FakeProgressStorage()
        pool = hz.CookieAccountPool([hz.CookieAccount("a1", "cookie-1")])
        note = {
            "id": "n1",
            "model_type": "note",
            "note_card": {"type": "normal"},
            "_comments_next_cursor": "resume-cursor",
        }

        result = hz.collect_note_result(
            0, 1, note, api, pool,
            max_comments_per_note=4,
            delay_seconds=0,
            base_path={"media": "media"},
            download_media_flag=False,
            storage=storage,
            keyword="植村秀",
            storage_lock=threading.Lock(),
        )

        self.assertIsNotNone(result)
        self.assertEqual(api.comment_cursors, ["resume-cursor"])
        self.assertEqual([comment["comment_id"] for comment in storage.comments], ["c3", "c4"])
        self.assertEqual(storage.progress["n1"]["comments_crawl_status"], "done")

    def test_collect_note_result_skips_detail_response_without_items(self):
        pool = hz.CookieAccountPool([hz.CookieAccount("a1", "cookie-1")])
        note = {"id": "n-missing", "model_type": "note", "note_card": {"type": "normal"}}

        result = hz.collect_note_result(
            0, 1, note, MissingItemsNoteApi(), pool,
            max_comments_per_note=0,
            delay_seconds=0,
            base_path={"media": "media"},
            download_media_flag=False,
        )

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
