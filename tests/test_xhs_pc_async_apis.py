import unittest

from apis.xhs_pc_async_apis import AsyncXHSApi


class FakeResponse:
    status = 200

    async def json(self, content_type=None):
        return {"success": True, "msg": "成功", "data": {}}


class FakeRequestContext:
    async def __aenter__(self):
        return FakeResponse()

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class FakeSession:
    def __init__(self):
        self.requests = []
        self.closed = False

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        return FakeRequestContext()

    async def close(self):
        self.closed = True


class FakeSigner:
    def generate_request_params(self, cookies_str, api, data="", method="POST"):
        cookies = dict(item.split("=", 1) for item in cookies_str.split("; "))
        return {"x-s": "signature"}, cookies, "{}"

    def generate_x_rap_param(self, api, data, app_id=None):
        return "rap"


class AsyncXhsPcApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_reuses_session_and_keeps_account_cookies_per_request(self):
        session = FakeSession()
        api = AsyncXHSApi(signer=FakeSigner(), session_factory=lambda: session)

        search_success, _, _ = await api.search_note("测试", "a1=one; web_session=first")
        note_success, _, _ = await api.get_note_info(
            "https://www.xiaohongshu.com/explore/note-id", "a1=two; web_session=second"
        )
        await api.close()

        self.assertTrue(search_success)
        self.assertTrue(note_success)
        self.assertEqual(len(session.requests), 2)
        self.assertEqual(session.requests[0][2]["cookies"]["web_session"], "first")
        self.assertEqual(session.requests[1][2]["cookies"]["web_session"], "second")
        self.assertTrue(session.closed)

