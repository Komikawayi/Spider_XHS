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


if __name__ == "__main__":
    unittest.main()
