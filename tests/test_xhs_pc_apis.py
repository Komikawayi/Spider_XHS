import threading
import unittest
from unittest.mock import patch

from apis.xhs_pc_apis import XHS_Apis


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.cookies = {"stale": "cookie"}
        self.requests = []
        self.closed = False

    def mount(self, prefix, adapter):
        pass

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        return self.response

    def close(self):
        self.closed = True


class XhsPcApisResponseTests(unittest.TestCase):
    @patch("apis.xhs_pc_apis.generate_x_rap_param", return_value="rap")
    @patch("apis.xhs_pc_apis.generate_request_params", return_value=({}, {}, "{}"))
    def test_search_note_accepts_success_response_without_msg(self, _params, _rap):
        session = FakeSession(FakeResponse({"code": 0, "success": True, "data": {}}))
        success, msg, res_json = XHS_Apis(session_factory=lambda: session).search_note("植村秀", "a1=fake")

        self.assertTrue(success)
        self.assertEqual(msg, "成功")
        self.assertEqual(res_json["data"], {})

    @patch("apis.xhs_pc_apis.generate_x_rap_param", return_value="rap")
    @patch("apis.xhs_pc_apis.generate_request_params", return_value=({}, {}, "{}"))
    def test_get_note_info_accepts_success_response_without_msg(self, _params, _rap):
        session = FakeSession(FakeResponse({"code": 0, "success": True, "data": {}}))
        success, msg, res_json = XHS_Apis(session_factory=lambda: session).get_note_info(
            "https://www.xiaohongshu.com/explore/n1", "a1=fake"
        )

        self.assertTrue(success)
        self.assertEqual(msg, "成功")
        self.assertEqual(res_json["data"], {})

    def test_session_is_reused_within_thread_and_cookies_are_cleared(self):
        sessions = []

        def session_factory():
            session = FakeSession(FakeResponse({"success": True, "data": {}}))
            sessions.append(session)
            return session

        api = XHS_Apis(session_factory=session_factory)
        api._request("GET", "https://example.test/one", "/one", cookies={"a1": "one"})
        api._request("GET", "https://example.test/two", "/two", cookies={"a1": "two"})

        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0].cookies, {})
        self.assertEqual(len(sessions[0].requests), 2)
        api.close()
        self.assertTrue(sessions[0].closed)

    def test_session_is_isolated_between_worker_threads(self):
        sessions = []
        lock = threading.Lock()

        def session_factory():
            session = FakeSession(FakeResponse({"success": True, "data": {}}))
            with lock:
                sessions.append(session)
            return session

        api = XHS_Apis(session_factory=session_factory)
        threads = [
            threading.Thread(
                target=api._request,
                args=("GET", f"https://example.test/{index}", "/test"),
                kwargs={"cookies": {"a1": str(index)}},
            )
            for index in range(2)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(sessions), 2)
        self.assertTrue(all(len(session.requests) == 1 for session in sessions))
        api.close()


if __name__ == "__main__":
    unittest.main()
