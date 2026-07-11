from concurrent.futures import ThreadPoolExecutor
import time

from xhs_utils.xhs_util import generate_request_params, generate_x_rap_param


class SigningExecutor:
    """限制 PyExecJS 签名任务并发，避免签名抢占采集线程的 CPU。"""

    def __init__(
        self,
        max_workers=2,
        metrics=None,
        request_params_fn=generate_request_params,
        rap_param_fn=generate_x_rap_param,
    ):
        if max_workers < 1:
            raise ValueError("max_workers 必须大于等于 1")
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="xhs-sign",
        )
        self._metrics = metrics
        self._request_params_fn = request_params_fn
        self._rap_param_fn = rap_param_fn

    def _run(self, func, *args):
        started_at = time.monotonic()
        try:
            result = self._executor.submit(func, *args).result()
        except Exception:
            if self._metrics:
                self._metrics.record_duration(
                    "signing", time.monotonic() - started_at, success=False
                )
            raise
        if self._metrics:
            self._metrics.record_duration(
                "signing", time.monotonic() - started_at, success=True
            )
        return result

    def generate_request_params(self, cookies_str, api, data="", method="POST"):
        return self._run(self._request_params_fn, cookies_str, api, data, method)

    def generate_x_rap_param(self, api, data, app_id=None):
        return self._run(self._rap_param_fn, api, data, app_id)

    def close(self):
        self._executor.shutdown(wait=True)
