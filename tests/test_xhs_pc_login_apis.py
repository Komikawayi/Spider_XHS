import unittest
from unittest.mock import patch

from apis.xhs_pc_login_apis import XHSLoginApi


class XhsPcLoginApiTests(unittest.TestCase):
    def test_apply_qrcode_login_info_accepts_session_variants(self):
        for payload in (
            {"login_info": {"session": "session-value"}},
            {"loginInfo": {"webSession": "session-value"}},
            {"web_session": "session-value"},
        ):
            cookies = {}

            applied = XHSLoginApi._apply_qrcode_login_info(cookies, payload)

            self.assertTrue(applied)
            self.assertEqual(cookies["web_session"], "session-value")

    def test_apply_qrcode_login_info_rejects_missing_session(self):
        cookies = {}

        applied = XHSLoginApi._apply_qrcode_login_info(cookies, {"login_info": {"user_id": "u1"}})

        self.assertFalse(applied)
        self.assertNotIn("web_session", cookies)

    @patch("apis.xhs_pc_login_apis.generate_headers", return_value=({}, ""))
    @patch("apis.xhs_pc_login_apis.requests.post")
    def test_qrcode_status_reads_session_from_userinfo_response(self, post, _headers):
        post.return_value.cookies = {}
        post.return_value.json.return_value = {
            "data": {"codeStatus": 2, "loginInfo": {"session": "session-value"}},
        }

        success, message, cookies = XHSLoginApi().check_qrcode_status("qr", "code", {"a1": "a1"})

        self.assertTrue(success)
        self.assertEqual(message, "验证成功")
        self.assertEqual(cookies["web_session"], "session-value")

    @patch("apis.xhs_pc_login_apis.generate_headers", return_value=({}, ""))
    @patch("apis.xhs_pc_login_apis.requests.get")
    def test_qrcode_status_uses_required_query_shape_and_login_mode_header(self, get, headers):
        get.return_value.cookies = {}
        get.return_value.json.return_value = {"success": True, "data": {}}

        XHSLoginApi()._login_by_qrcode_status("qr-value", "code-value", {"a1": "a1"})

        headers.assert_called_once_with(
            "a1", "/api/sns/web/v1/login/qrcode/status?qr_id=qr-value&code=code-value", method="GET"
        )
        self.assertEqual(get.call_args.kwargs["headers"]["x-login-mode"], "")


if __name__ == "__main__":
    unittest.main()
