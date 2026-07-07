import unittest
from unittest.mock import patch

from apis.xhs_pc_apis import XHS_Apis


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class XhsPcApisResponseTests(unittest.TestCase):
    @patch("apis.xhs_pc_apis.generate_x_rap_param", return_value="rap")
    @patch("apis.xhs_pc_apis.generate_request_params", return_value=({}, {}, "{}"))
    @patch("apis.xhs_pc_apis.requests.post", return_value=FakeResponse({"code": 0, "success": True, "data": {}}))
    def test_search_note_accepts_success_response_without_msg(self, _post, _params, _rap):
        success, msg, res_json = XHS_Apis().search_note("植村秀", "a1=fake")

        self.assertTrue(success)
        self.assertEqual(msg, "成功")
        self.assertEqual(res_json["data"], {})

    @patch("apis.xhs_pc_apis.generate_x_rap_param", return_value="rap")
    @patch("apis.xhs_pc_apis.generate_request_params", return_value=({}, {}, "{}"))
    @patch("apis.xhs_pc_apis.requests.post", return_value=FakeResponse({"code": 0, "success": True, "data": {}}))
    def test_get_note_info_accepts_success_response_without_msg(self, _post, _params, _rap):
        success, msg, res_json = XHS_Apis().get_note_info("https://www.xiaohongshu.com/explore/n1", "a1=fake")

        self.assertTrue(success)
        self.assertEqual(msg, "成功")
        self.assertEqual(res_json["data"], {})


if __name__ == "__main__":
    unittest.main()
