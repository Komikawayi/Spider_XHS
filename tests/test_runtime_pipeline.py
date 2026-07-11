import json
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from scripts import spider_hangzhou_ecommerce as hz
from xhs_utils.crawl_metrics import CrawlMetrics
from xhs_utils.signing_executor import SigningExecutor
from xhs_utils.storage_writer import StorageWriter


class FakeBatchStorage:
    def __init__(self, dsn):
        self.events = []
        self.counts = {}
        self.closed = False

    def write_batch(self, notes, comment_pages, progress_updates):
        for item in notes:
            self.events.append(("note", item["note"]["note_id"]))
        for page in comment_pages:
            self.events.append(("comments", page["note_id"], len(page["comments"])))
            self.counts[page["note_id"]] = self.counts.get(page["note_id"], 0) + len(page["comments"])
        self.events.append(("count", tuple(page["note_id"] for page in comment_pages)))
        results = {}
        for page in comment_pages:
            count = self.counts[page["note_id"]]
            is_done = count >= page["target_count"] or not page["has_more"] or not page["next_cursor"]
            results[page["note_id"]] = {"collected_count": count, "is_done": is_done}
            self.events.append(("progress", page["note_id"], "done" if is_done else "partial"))
        for item in progress_updates:
            self.events.append(("progress", item["note_id"], item["status"]))
        return results

    def close(self):
        self.closed = True


class RuntimePipelineTests(unittest.TestCase):
    def test_effective_note_concurrency_is_capped_by_cookie_capacity(self):
        pool = hz.CookieAccountPool([
            hz.CookieAccount("a1", "cookie", max_concurrency=2),
        ])

        self.assertEqual(hz.effective_note_concurrency(8, pool), 2)

    def test_metrics_records_account_wait_and_snapshot(self):
        metrics = CrawlMetrics()
        pool = hz.CookieAccountPool([hz.CookieAccount("a1", "cookie")], metrics=metrics)

        account = pool.acquire()
        pool.report_success(account)
        metrics.record_duration("signing", 0.01, success=True)

        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["timings"]["account.wait"]["count"], 1)
        self.assertEqual(snapshot["counters"]["signing.success"], 1)

    def test_metrics_writes_structured_json_record(self):
        metrics = CrawlMetrics()
        metrics.increment("notes.completed", 2)

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "metrics.json"
            metrics.write_json(output, {"status": "done"})
            payload = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(payload["counters"]["notes.completed"], 2)
        self.assertEqual(payload["metadata"]["status"], "done")

    def test_signing_executor_limits_parallel_calls(self):
        active = 0
        max_active = 0
        lock = threading.Lock()

        def sign(*args):
            nonlocal active, max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.03)
            with lock:
                active -= 1
            return {}, {}, ""

        signer = SigningExecutor(max_workers=2, request_params_fn=sign)
        try:
            with ThreadPoolExecutor(max_workers=6) as executor:
                list(executor.map(
                    lambda _: signer.generate_request_params("cookie", "/api", "", "GET"),
                    range(6),
                ))
        finally:
            signer.close()

        self.assertLessEqual(max_active, 2)

    def test_storage_writer_writes_note_before_comment_page_and_progress(self):
        instances = []

        def storage_factory(dsn):
            storage = FakeBatchStorage(dsn)
            instances.append(storage)
            return storage

        writer = StorageWriter(
            "fake-dsn",
            batch_size=10,
            flush_seconds=0.01,
            storage_factory=storage_factory,
        ).start()
        try:
            writer.submit_note({"note_id": "n1"}, keyword="keyword", raw={})
            page = writer.submit_comment_page(
                "n1",
                "title",
                [({"comment_id": "c1"}, {"id": "c1"})],
                target_count=2,
                next_cursor="next",
                has_more=True,
            )
            result = page.result(timeout=1)
            writer.flush()
        finally:
            writer.close()

        storage = instances[0]
        self.assertEqual(result, {"collected_count": 1, "is_done": False})
        self.assertEqual([event[0] for event in storage.events[:4]], [
            "note", "comments", "count", "progress",
        ])
        self.assertTrue(storage.closed)


if __name__ == "__main__":
    unittest.main()
