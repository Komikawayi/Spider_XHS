import json
import tempfile
import unittest
from pathlib import Path

from scripts import manage_cookies


class FakeLoginApi:
    def __init__(self, cookies):
        self.cookies = list(cookies)
        self.qrcode_calls = []

    def qrcode_login(self, show_in_terminal=True):
        self.qrcode_calls.append(show_in_terminal)
        return self.cookies.pop(0)

    def get_user_info(self, cookies):
        return True, {"nickname": "nick", "red_id": "red"}, cookies


class FakeCrawlApi:
    def __init__(self, success=True, message="成功"):
        self.success = success
        self.message = message

    def search_note(self, query, cookies, page=1):
        return self.success, self.message, {"data": {}}


class ManageCookiesTests(unittest.TestCase):
    def test_login_qr_batch_appends_accounts_with_next_available_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "cookies.local.json"
            output.write_text(json.dumps([
                {"name": "account_1", "cookies": "old-cookie", "status": "active"}
            ]), encoding="utf-8")
            fake_api = FakeLoginApi([
                "a1=one; web_session=session-1",
                "a1=two; web_session=session-2",
            ])

            records = manage_cookies.login_qr_batch(
                count=2,
                output_path=output,
                login_api=fake_api,
                show_in_terminal=True,
            )

            saved = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual([record["name"] for record in records], ["account_1", "account_2", "account_3"])
        self.assertEqual(
            [record["cookies"] for record in saved],
            ["old-cookie", "a1=one; web_session=session-1", "a1=two; web_session=session-2"],
        )
        self.assertEqual(saved[1]["status"], "active")
        self.assertEqual(saved[1]["daily_note_limit"], 150)
        self.assertEqual(saved[1]["daily_comment_limit"], 2500)
        self.assertEqual(
            saved[1]["max_concurrency"], manage_cookies.DEFAULT_ACCOUNT_CONCURRENCY_PER_ACCOUNT
        )
        self.assertEqual(saved[1]["note_ids_today"], [])
        self.assertEqual(saved[1]["comments_today"], 0)
        self.assertEqual(saved[1]["nickname"], "nick")
        self.assertEqual(saved[1]["red_id"], "red")
        self.assertEqual(fake_api.qrcode_calls, [True, True])

    def test_build_cookie_record_marks_cookie_without_web_session_expired(self):
        fake_api = FakeLoginApi([])

        record = manage_cookies.build_cookie_record("bad", "a1=abc; acw_tc=token", fake_api)

        self.assertEqual(record["status"], "expired")
        self.assertIn("web_session", record["last_error"])

    def test_build_cookie_record_marks_unverified_session_expired(self):
        class UnverifiedLoginApi(FakeLoginApi):
            def get_user_info(self, cookies):
                return False, {}, cookies

        record = manage_cookies.build_cookie_record(
            "unverified", "a1=abc; web_session=token", UnverifiedLoginApi([])
        )

        self.assertEqual(record["status"], "expired")
        self.assertEqual(record["last_error"], "获取用户信息失败")

    def test_import_cookie_saves_only_verified_account(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "cookies.local.json"
            output.write_text(json.dumps([{"name": "account_1", "cookies": "old"}]), encoding="utf-8")

            record = manage_cookies.import_cookie(
                "a1=one; web_session=session-1",
                output_path=output,
                login_api=FakeLoginApi([]),
                crawl_api=FakeCrawlApi(),
            )
            saved = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(record["name"], "account_2")
        self.assertEqual([item["name"] for item in saved], ["account_1", "account_2"])
        self.assertEqual(saved[-1]["status"], "active")

    def test_import_cookie_rejects_duplicate_account(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "cookies.local.json"
            output.write_text(json.dumps([
                {"name": "account_1", "status": "active", "red_id": "red"},
            ]), encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "account_1"):
                manage_cookies.import_cookie(
                    "a1=one; web_session=session-1", output_path=output, login_api=FakeLoginApi([])
                )

            saved = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual([item["name"] for item in saved], ["account_1"])

    def test_import_cookie_rejects_cookie_without_crawl_access(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "cookies.local.json"

            with self.assertRaisesRegex(RuntimeError, "采集接口验证失败"):
                manage_cookies.import_cookie(
                    "a1=one; web_session=session-1",
                    output_path=output,
                    login_api=FakeLoginApi([]),
                    crawl_api=FakeCrawlApi(success=False, message="登录已过期"),
                )

        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
