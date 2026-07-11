from concurrent.futures import Future
import queue
import threading
import time

from xhs_utils.pg_storage import PostgresStorage


class StorageWriter:
    """用独立连接批量写入 PostgreSQL，避免采集线程等待单条 SQL。"""

    _STOP = object()

    def __init__(
        self,
        dsn,
        batch_size=100,
        flush_seconds=0.05,
        max_queue_size=1000,
        metrics=None,
        storage_factory=PostgresStorage,
    ):
        if batch_size < 1:
            raise ValueError("batch_size 必须大于等于 1")
        if flush_seconds <= 0:
            raise ValueError("flush_seconds 必须大于 0")
        self._dsn = dsn
        self._batch_size = batch_size
        self._flush_seconds = flush_seconds
        self._metrics = metrics
        self._storage_factory = storage_factory
        self._queue = queue.Queue(maxsize=max_queue_size)
        self._ready = threading.Event()
        self._failure_lock = threading.Lock()
        self._failure = None
        self._started = False
        self._thread = threading.Thread(
            target=self._run,
            name="xhs-db-writer",
            daemon=True,
        )

    def start(self):
        if self._started:
            return self
        self._started = True
        self._thread.start()
        self._ready.wait()
        self.raise_if_failed()
        return self

    def _set_failure(self, error):
        with self._failure_lock:
            if self._failure is None:
                self._failure = error

    def raise_if_failed(self):
        with self._failure_lock:
            failure = self._failure
        if failure is not None:
            raise RuntimeError(f"数据库批量写入器失败: {failure}") from failure

    def _submit(self, operation):
        if not self._started:
            raise RuntimeError("数据库批量写入器尚未启动")
        self.raise_if_failed()
        self._queue.put(operation)
        if self._metrics:
            self._metrics.observe_max("database.queue_depth", self._queue.qsize())

    def submit_note(self, note, keyword="", raw=None):
        self._submit({
            "kind": "note",
            "note": note,
            "keyword": keyword,
            "raw": raw,
        })

    def submit_comment_page(
        self,
        note_id,
        note_title,
        comments,
        target_count,
        next_cursor,
        has_more,
    ):
        result = Future()
        self._submit({
            "kind": "comment_page",
            "note_id": note_id,
            "note_title": note_title,
            "comments": comments,
            "target_count": target_count,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "future": result,
        })
        return result

    def submit_progress(
        self,
        note_id,
        status,
        collected_count,
        target_count,
        next_cursor,
        has_more,
    ):
        result = Future()
        self._submit({
            "kind": "progress",
            "note_id": note_id,
            "status": status,
            "collected_count": collected_count,
            "target_count": target_count,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "future": result,
        })
        return result

    @staticmethod
    def _finish_operation(operation, error=None, result=None):
        future = operation.get("future")
        if not future or future.done():
            return
        if error is None:
            future.set_result(result)
        else:
            future.set_exception(error)

    def _flush_batch(self, storage, batch):
        if not batch:
            return
        started_at = time.monotonic()
        notes = [item for item in batch if item["kind"] == "note"]
        comment_pages = [item for item in batch if item["kind"] == "comment_page"]
        progress_updates = [item for item in batch if item["kind"] == "progress"]
        try:
            page_results = storage.write_batch(notes, comment_pages, progress_updates)
        except Exception as error:
            self._set_failure(error)
            for item in batch:
                self._finish_operation(item, error=error)
            raise

        for page in comment_pages:
            self._finish_operation(page, result=page_results[page["note_id"]])
        for update in progress_updates:
            self._finish_operation(update, result=None)
        if self._metrics:
            row_count = len(notes) + len(progress_updates) + sum(
                len(page["comments"]) for page in comment_pages
            )
            self._metrics.increment("database.batches")
            self._metrics.increment("database.rows", row_count)
            self._metrics.record_duration(
                "database.write_batch", time.monotonic() - started_at, success=True
            )

    def _flush_or_fail(self, storage, batch):
        if not batch:
            return
        try:
            self._flush_batch(storage, batch)
        except Exception:
            if self._metrics:
                self._metrics.record_duration("database.write_batch", 0, success=False)

    def _run(self):
        storage = None
        batch = []
        try:
            storage = self._storage_factory(self._dsn)
        except Exception as error:
            self._set_failure(error)
            self._ready.set()
            return
        self._ready.set()

        try:
            while True:
                try:
                    operation = self._queue.get(
                        timeout=self._flush_seconds if batch else None
                    )
                except queue.Empty:
                    self._flush_or_fail(storage, batch)
                    batch = []
                    continue

                if operation is self._STOP:
                    self._flush_or_fail(storage, batch)
                    break
                if operation["kind"] == "flush":
                    self._flush_or_fail(storage, batch)
                    batch = []
                    self._finish_operation(operation, error=self._failure)
                    continue
                if self._failure is not None:
                    self._finish_operation(operation, error=self._failure)
                    continue

                batch.append(operation)
                if len(batch) >= self._batch_size:
                    self._flush_or_fail(storage, batch)
                    batch = []
        finally:
            if storage:
                storage.close()

    def flush(self):
        if not self._started:
            return
        if not self._thread.is_alive():
            self.raise_if_failed()
            return
        result = Future()
        self._queue.put({"kind": "flush", "future": result})
        result.result()
        self.raise_if_failed()

    def close(self):
        if not self._started:
            return
        error = None
        try:
            self.flush()
        except Exception as exc:
            error = exc
        finally:
            self._queue.put(self._STOP)
            self._thread.join()
        if error:
            raise error
        self.raise_if_failed()
