# -*- coding: utf-8 -*-
"""aiohttp + asyncio 流式采集入口；线程版脚本保留为稳定回退。"""

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apis.xhs_pc_async_apis import AsyncXHSApi
from scripts.spider_hangzhou_ecommerce import (
    NoAvailableCookieAccounts,
    StopCrawl,
    build_note_url,
    build_task_output_dir,
    classify_account_failure,
    iter_image_notes,
    load_cookie_accounts,
    parse_comment,
    parse_note_url_context,
    resolve_cookies_file,
    setup_run_logging,
    should_stop_for_message,
)
from xhs_utils.common_util import init
from xhs_utils.crawl_metrics import CrawlMetrics
from xhs_utils.data_util import handle_note_info, save_to_xlsx
from xhs_utils.pg_storage import PostgresStorage
from xhs_utils.signing_executor import SigningExecutor
from xhs_utils.storage_writer import StorageWriter
from xhs_utils.xhs_util import generate_search_id


DEFAULT_QUERY = "植村秀"
DEFAULT_REQUIRE_NUM = 200
DEFAULT_TARGET_COMMENTS = 2000
DEFAULT_MAX_COMMENTS_PER_NOTE = 100
DEFAULT_NOTE_CONCURRENCY = 20
DEFAULT_COMMENT_CONCURRENCY = 12
DEFAULT_SIGN_CONCURRENCY = 12
DEFAULT_MAX_SEARCH_PAGES = 50
DEFAULT_DB_BATCH_SIZE = 100
DEFAULT_DB_FLUSH_SECONDS = 0.05
DEFAULT_LOG_DIR = "logs"


class AsyncAccountPool:
    """原生 asyncio 账号调度；复用线程版账号模型和安全写回逻辑。"""

    def __init__(self, pool, metrics=None, ignore_quotas=False, runtime_concurrency=None):
        self._pool = pool
        self.accounts = pool.accounts
        self.metrics = metrics or pool.metrics
        self.ignore_quotas = ignore_quotas
        self.runtime_concurrency = runtime_concurrency
        self._condition = asyncio.Condition()

    def _limit(self, account):
        return self.runtime_concurrency or account.max_concurrency

    def active_capacity(self):
        return sum(self._limit(a) for a in self.accounts if a.status == "active")

    def has_active(self):
        return any(a.status == "active" for a in self.accounts)

    def _load_key(self, account):
        concurrency_ratio = account.active_count / max(1, self._limit(account))
        note_ratio = len(account.note_ids_today) / max(1, account.daily_note_limit)
        comment_ratio = account.comments_today / max(1, account.daily_comment_limit)
        return concurrency_ratio, max(note_ratio, comment_ratio), account.name

    async def _acquire_matching(self, predicate, exhausted_message):
        started_at = time.monotonic()
        acquired = False
        try:
            async with self._condition:
                while True:
                    matching = [
                        a for a in self.accounts
                        if a.status == "active" and predicate(a)
                    ]
                    available = [
                        a for a in matching if a.active_count < self._limit(a)
                    ]
                    if available:
                        account = min(available, key=self._load_key)
                        account.active_count += 1
                        acquired = True
                        if self.metrics:
                            self.metrics.increment(f"account.{account.name}.acquired")
                            self.metrics.mark_event(f"account.{account.name}.requests")
                            self.metrics.observe_max(
                                f"account.{account.name}.active", account.active_count
                            )
                        return account
                    if matching and any(a.active_count > 0 for a in matching):
                        await self._condition.wait()
                        continue
                    raise NoAvailableCookieAccounts(exhausted_message)
        finally:
            if self.metrics:
                self.metrics.record_duration(
                    "account.wait", time.monotonic() - started_at, success=acquired
                )

    async def acquire(self):
        return await self._acquire_matching(
            lambda _: True,
            "没有可用账号：所有 Cookie 均已过期或进入冷却",
        )

    async def acquire_for_note(self, note_id):
        return await self._acquire_matching(
            lambda a: self.ignore_quotas
            or note_id in a.note_ids_today
            or len(a.note_ids_today) < a.daily_note_limit,
            "没有可用账号：所有 Cookie 均已过期、进入冷却或达到每日图文额度",
        )

    async def acquire_for_comments(self):
        return await self._acquire_matching(
            lambda a: self.ignore_quotas or a.comments_today < a.daily_comment_limit,
            "没有可用账号：所有 Cookie 均已过期、进入冷却或达到每日评论额度",
        )

    async def _persist(self, account):
        await asyncio.to_thread(self._pool._persist_usage, account)

    async def report_success(self, account):
        async with self._condition:
            account.active_count = max(0, account.active_count - 1)
            self._condition.notify_all()

    async def report_note_success(self, account, note_id):
        async with self._condition:
            account.note_ids_today.add(note_id)
            account.active_count = max(0, account.active_count - 1)
            self._condition.notify_all()
        if self.metrics:
            self.metrics.increment(f"account.{account.name}.notes")
        await self._persist(account)

    async def report_comments_success(self, account, count):
        async with self._condition:
            account.comments_today += max(0, int(count or 0))
            account.active_count = max(0, account.active_count - 1)
            self._condition.notify_all()
        if self.metrics:
            self.metrics.increment(f"account.{account.name}.comments", count)
        await self._persist(account)

    async def report_failure(self, account, message):
        status = classify_account_failure(message) or "cooling"
        async with self._condition:
            account.status = status
            account.active_count = max(0, account.active_count - 1)
            self._condition.notify_all()
        if self.metrics:
            self.metrics.increment(f"account.{account.name}.failure")
            self.metrics.increment(f"account.failure.{status}")
        await self._persist(account)


@dataclass
class PipelineState:
    target_notes: int
    target_comments: int
    retain_rows: bool = False
    note_count: int = 0
    comment_count: int = 0
    stop_reason: str = ""
    search_exhausted: bool = False
    fatal_error: Exception | None = None
    note_rows: list = field(default_factory=list)
    comment_rows: list = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def claim_note(self, note_data):
        async with self.lock:
            if self.note_count >= self.target_notes:
                return False
            self.note_count += 1
            if self.retain_rows:
                self.note_rows.append(note_data)
            return True

    async def claim_comments(self, rows):
        async with self.lock:
            remaining = max(0, self.target_comments - self.comment_count)
            accepted = rows[:remaining]
            self.comment_count += len(accepted)
            if self.retain_rows:
                self.comment_rows.extend(row[0] for row in accepted)
            return accepted

    def note_target_reached(self):
        return self.note_count >= self.target_notes

    def comment_target_reached(self):
        return self.comment_count >= self.target_comments


async def wait_writer(future):
    return await asyncio.wrap_future(future)


async def queue_put(queue, item, name, metrics):
    started_at = time.monotonic()
    await queue.put(item)
    metrics.record_duration(f"queue.{name}.put_wait", time.monotonic() - started_at)
    metrics.observe_max(f"queue.{name}.depth", queue.qsize())


async def queue_get(queue, name, metrics):
    started_at = time.monotonic()
    item = await queue.get()
    metrics.record_duration(f"queue.{name}.get_wait", time.monotonic() - started_at)
    return item


async def verify_account(api, account, stage, metrics, query):
    user_success, user_message, user_payload = await api.get_user_me(account.cookies)
    search_success, search_message, search_payload = await api.search_note(
        query, account.cookies, page=1, note_type=2, search_id=generate_search_id()
    )
    success = user_success and search_success
    message = user_message if not user_success else search_message
    metrics.increment(f"validation.{stage}.success" if success else f"validation.{stage}.failure")
    return {
        "account": account.name,
        "success": bool(success),
        "message": message,
        "user_response_keys": sorted((user_payload or {}).keys()),
        "search_response_keys": sorted((search_payload or {}).keys()),
    }


async def load_progress(storage, notes, max_comments, ignore_existing):
    if not storage or ignore_existing or not notes:
        return notes
    note_ids = [note["id"] for note in notes]
    progress_by_id = await asyncio.to_thread(
        storage.get_comment_progress_by_note_ids, note_ids
    )
    filtered = []
    for note in notes:
        progress = progress_by_id.get(note["id"])
        if progress:
            done = progress.get("comments_crawl_status") == "done"
            target_met = int(progress.get("comments_target_count") or 0) >= max_comments
            no_more = progress.get("comments_has_more") is False
            if done and (target_met or no_more):
                continue
            note = dict(note)
            note["_comments_next_cursor"] = progress.get("comments_next_cursor") or ""
            note["_comments_collected_count"] = int(
                progress.get("comments_collected_count") or 0
            )
        filtered.append(note)
    return filtered


async def search_producer(api, pool, args, note_queue, state, metrics, storage=None):
    root_search_id = generate_search_id()
    seen = set()
    empty_pages = 0
    page = 1
    while page <= args.max_search_pages and not state.note_target_reached():
        try:
            account = await pool.acquire()
        except NoAvailableCookieAccounts as error:
            state.fatal_error = error
            return
        try:
            success, message, payload = await api.search_note(
                args.query,
                account.cookies,
                page=page,
                sort_type_choice=args.sort_type_choice,
                note_type=2,
                note_time=args.note_time,
                note_range=args.note_range,
                search_id=root_search_id,
            )
        except Exception as error:
            await pool.report_success(account)
            state.fatal_error = error
            return
        if not success:
            if should_stop_for_message(message):
                await pool.report_failure(account, message)
                if pool.has_active():
                    continue
                state.fatal_error = StopCrawl(message)
                return
            await pool.report_success(account)
            state.fatal_error = RuntimeError(f"搜索失败: {message}")
            return
        await pool.report_success(account)

        metrics.increment("search.pages")
        data = (payload or {}).get("data") or {}
        page_items = data.get("items") or []
        metrics.increment("search.candidates.raw", len(page_items))
        if not page_items:
            empty_pages += 1
            metrics.increment("search.empty_pages")
        else:
            empty_pages = 0

        candidates = []
        for note in iter_image_notes(page_items):
            note_id = note["id"]
            if note_id in seen:
                metrics.increment("search.candidates.duplicate")
                continue
            seen.add(note_id)
            candidates.append(note)
        metrics.increment("search.candidates.unique", len(candidates))
        metrics.increment("search.candidates.filtered", len(page_items) - len(candidates))
        candidates = await load_progress(
            storage, candidates, args.max_comments_per_note,
            args.ignore_existing_progress,
        )
        for note in candidates:
            if state.note_target_reached():
                break
            await queue_put(note_queue, note, "note", metrics)

        if not data.get("has_more") or empty_pages >= 2:
            state.search_exhausted = True
            break
        page += 1

    if page > args.max_search_pages:
        metrics.increment("search.max_pages_reached")


async def detail_worker(api, pool, args, note_queue, comment_queue, state, writer, metrics):
    while True:
        note = await queue_get(note_queue, "note", metrics)
        try:
            if note is None:
                return
            if state.fatal_error or state.note_target_reached():
                continue
            note_id = note["id"]
            note_url = build_note_url(note)
            metrics.increment("notes.started")
            try:
                account = await pool.acquire_for_note(note_id)
            except NoAvailableCookieAccounts as error:
                state.fatal_error = error
                continue
            try:
                metrics.change_gauge("workers.detail.active", 1)
                success, message, payload = await api.get_note_info(
                    note_url, account.cookies
                )
            except Exception as error:
                await pool.report_success(account)
                metrics.increment("notes.request_error")
                logger.warning(f"笔记详情异常 {note_id}: {error}")
                continue
            finally:
                metrics.change_gauge("workers.detail.active", -1)
            if not success:
                if should_stop_for_message(message):
                    await pool.report_failure(account, message)
                    if not pool.has_active():
                        state.fatal_error = StopCrawl(message)
                else:
                    await pool.report_success(account)
                    metrics.increment("notes.response_failure")
                continue

            items = ((payload or {}).get("data") or {}).get("items") or []
            if not items:
                await pool.report_success(account)
                metrics.increment("notes.unavailable")
                metrics.increment("response.schema.note_items_missing")
                continue
            raw_note = items[0]
            raw_note["url"] = note_url
            note_data = handle_note_info(raw_note)
            if note_data.get("note_type") != "图集":
                await pool.report_success(account)
                metrics.increment("notes.filtered")
                continue
            if not await state.claim_note(note_data):
                await pool.report_success(account)
                continue
            await pool.report_note_success(account, note_id)
            if writer:
                await asyncio.to_thread(writer.submit_note, note_data, args.query, raw_note)
            metrics.increment("notes.completed")
            if state.note_count in (50, 100, 150, 200, 250, 300, 400):
                metrics.increment(f"milestone.notes.{state.note_count}")
                metrics.mark(
                    f"notes.{state.note_count}",
                    {"http_requests": metrics.get_counter("http.requests.total")},
                )

            collected = int(note.get("_comments_collected_count") or 0)
            cursor = note.get("_comments_next_cursor") or ""
            if args.max_comments_per_note <= collected or state.comment_target_reached():
                continue
            await queue_put(comment_queue, {
                "note_id": note_id,
                "note_url": note_url,
                "note_title": note_data.get("title", "无标题"),
                "cursor": cursor,
                "collected": collected,
            }, "comment", metrics)
        except Exception as error:
            metrics.increment("workers.detail.unexpected_failure")
            logger.exception(f"详情 worker 未预期异常: {error}")
            state.fatal_error = error
        finally:
            note_queue.task_done()


async def comment_worker(api, pool, args, comment_queue, state, writer, metrics):
    while True:
        job = await queue_get(comment_queue, "comment", metrics)
        try:
            if job is None:
                return
            if state.fatal_error or state.comment_target_reached():
                continue
            try:
                account = await pool.acquire_for_comments()
            except NoAvailableCookieAccounts as error:
                state.fatal_error = error
                continue
            _, xsec_token = parse_note_url_context(job["note_url"])
            try:
                metrics.change_gauge("workers.comment.active", 1)
                success, message, payload = await api.get_note_out_comment(
                    job["note_id"], job["cursor"], xsec_token, account.cookies
                )
            except Exception as error:
                await pool.report_success(account)
                metrics.increment("comments.request_error")
                logger.warning(f"评论页异常 {job['note_id']}: {error}")
                continue
            finally:
                metrics.change_gauge("workers.comment.active", -1)
            if not success:
                if should_stop_for_message(message):
                    await pool.report_failure(account, message)
                    if pool.has_active():
                        await queue_put(comment_queue, job, "comment", metrics)
                    else:
                        state.fatal_error = StopCrawl(message)
                else:
                    await pool.report_success(account)
                    metrics.increment("comments.response_failure")
                continue

            data = (payload or {}).get("data") or {}
            comments = data.get("comments") or []
            next_cursor = str(data.get("cursor") or "")
            has_more = bool(data.get("has_more"))
            per_note_remaining = max(0, args.max_comments_per_note - job["collected"])
            parsed = [
                (parse_comment(comment, job["note_id"], job["note_title"]), comment)
                for comment in comments[:per_note_remaining]
            ]
            accepted = await state.claim_comments(parsed)
            await pool.report_comments_success(account, len(accepted))
            metrics.increment("comments.pages")
            metrics.increment("comments.fetched", len(comments))
            metrics.increment("comments.saved", len(accepted))

            if writer:
                future = await asyncio.to_thread(
                    writer.submit_comment_page,
                    job["note_id"], job["note_title"], accepted,
                    args.max_comments_per_note, next_cursor, has_more,
                )
                progress = await wait_writer(future)
                job["collected"] = progress["collected_count"]
                done = progress["is_done"]
            else:
                job["collected"] += len(accepted)
                done = (
                    job["collected"] >= args.max_comments_per_note
                    or not has_more or not next_cursor
                )
            if state.comment_count in (500, 1000, 1500, 2000, 2500, 3000, 4000):
                metrics.increment(f"milestone.comments.{state.comment_count}")
                metrics.mark(
                    f"comments.{state.comment_count}",
                    {"http_requests": metrics.get_counter("http.requests.total")},
                )
            if not done and not state.comment_target_reached():
                job["cursor"] = next_cursor
                metrics.increment("comments.requeued")
                await queue_put(comment_queue, job, "comment", metrics)
        except Exception as error:
            metrics.increment("workers.comment.unexpected_failure")
            logger.exception(f"评论 worker 未预期异常: {error}")
            state.fatal_error = error
        finally:
            comment_queue.task_done()


async def resource_sampler(metrics, stop_event):
    try:
        import psutil
    except ImportError:
        metrics.increment("resource.psutil_unavailable")
        return
    process = psutil.Process()
    process.cpu_percent(None)
    while not stop_event.is_set():
        try:
            children = [c for c in process.children(recursive=True) if "node" in c.name().lower()]
            child_cpu = sum(c.cpu_percent(None) for c in children)
            rss_mb = process.memory_info().rss / 1024 / 1024
            metrics.record_value("resource.process_cpu_percent", process.cpu_percent(None))
            metrics.record_value("resource.node_cpu_percent", child_cpu)
            metrics.record_value("resource.rss_mb", rss_mb)
            metrics.observe_max("resource.threads", process.num_threads())
            metrics.observe_max("resource.node_processes", len(children))
        except (psutil.Error, OSError):
            metrics.increment("resource.sample_failure")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass


async def run_pipeline(api, pool, args, writer, metrics, storage=None, initial_notes=None):
    note_queue = asyncio.Queue(maxsize=max(1, args.note_concurrency * 4))
    comment_queue = asyncio.Queue(maxsize=max(1, args.comment_concurrency * 4))
    state = PipelineState(
        target_notes=args.require_num,
        target_comments=args.target_comments,
        retain_rows=writer is None,
    )
    resource_stop = asyncio.Event()
    sampler = asyncio.create_task(resource_sampler(metrics, resource_stop))
    detail_tasks = [
        asyncio.create_task(
            detail_worker(api, pool, args, note_queue, comment_queue, state, writer, metrics)
        )
        for _ in range(args.note_concurrency)
    ]
    comment_tasks = [
        asyncio.create_task(
            comment_worker(api, pool, args, comment_queue, state, writer, metrics)
        )
        for _ in range(args.comment_concurrency)
    ]
    metrics.observe_max("scheduler.async_note_workers", args.note_concurrency)
    metrics.observe_max("scheduler.async_comment_workers", args.comment_concurrency)
    try:
        if initial_notes is None:
            try:
                await search_producer(api, pool, args, note_queue, state, metrics, storage)
            except Exception as error:
                metrics.increment("workers.search.unexpected_failure")
                logger.exception(f"搜索生产者未预期异常: {error}")
                state.fatal_error = error
        else:
            for note in initial_notes:
                await queue_put(note_queue, note, "note", metrics)
            state.search_exhausted = True
        await note_queue.join()
        for _ in detail_tasks:
            await note_queue.put(None)
        await asyncio.gather(*detail_tasks)
        await comment_queue.join()
        for _ in comment_tasks:
            await comment_queue.put(None)
        await asyncio.gather(*comment_tasks)
    finally:
        resource_stop.set()
        await sampler

    if state.fatal_error:
        state.stop_reason = "accounts_unavailable" if isinstance(
            state.fatal_error, (NoAvailableCookieAccounts, StopCrawl)
        ) else "failed"
        raise state.fatal_error
    if state.note_target_reached():
        state.stop_reason = "note_target_reached"
    elif state.search_exhausted:
        state.stop_reason = "search_exhausted"
    else:
        state.stop_reason = "done"
    if state.comment_target_reached():
        state.stop_reason += "+comment_target_reached"
    return state


async def run_workers(api, pool, notes, args, writer, metrics):
    """兼容旧测试和调用方：对给定笔记列表运行新版双队列。"""
    if not hasattr(args, "require_num"):
        args.require_num = len(notes)
    if not hasattr(args, "target_comments"):
        args.target_comments = 10 ** 9
    if not hasattr(args, "comment_concurrency"):
        args.comment_concurrency = args.note_concurrency
    state = await run_pipeline(api, pool, args, writer, metrics, initial_notes=notes)
    return [{"note_data": row, "comments": []} for row in state.note_rows]


def build_parser():
    parser = argparse.ArgumentParser(description="小红书 aiohttp + asyncio 流式采集管线")
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--require-num", type=int, default=DEFAULT_REQUIRE_NUM)
    parser.add_argument("--target-comments", type=int, default=DEFAULT_TARGET_COMMENTS)
    parser.add_argument("--max-comments-per-note", type=int, default=DEFAULT_MAX_COMMENTS_PER_NOTE)
    parser.add_argument("--note-concurrency", type=int, default=DEFAULT_NOTE_CONCURRENCY)
    parser.add_argument("--comment-concurrency", type=int, default=DEFAULT_COMMENT_CONCURRENCY)
    parser.add_argument("--account-concurrency", type=int, default=None)
    parser.add_argument("--sign-concurrency", type=int, default=DEFAULT_SIGN_CONCURRENCY)
    parser.add_argument("--max-search-pages", type=int, default=DEFAULT_MAX_SEARCH_PAGES)
    parser.add_argument("--sort-type-choice", type=int, default=0)
    parser.add_argument("--note-time", type=int, default=0)
    parser.add_argument("--note-range", type=int, default=0)
    parser.add_argument("--account-name", default="")
    parser.add_argument("--calibration-mode", action="store_true")
    parser.add_argument("--fixed-concurrency", type=int, default=None)
    parser.add_argument("--ignore-existing-progress", action="store_true")
    parser.add_argument("--db-batch-size", type=int, default=DEFAULT_DB_BATCH_SIZE)
    parser.add_argument("--db-flush-seconds", type=float, default=DEFAULT_DB_FLUSH_SECONDS)
    parser.add_argument("--cookies-file", default="")
    parser.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
    parser.add_argument("--save-excel", action="store_true")
    parser.add_argument("--use-postgres", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def account_snapshot(accounts):
    return {
        account.name: {
            "status": account.status,
            "notes_used": len(account.note_ids_today),
            "comments_used": account.comments_today,
            "max_concurrency": account.max_concurrency,
        }
        for account in accounts
    }


def write_calibration_report(path, metadata, metrics_snapshot):
    http_failures = sum(
        value for key, value in metrics_snapshot["counters"].items()
        if key.startswith("http.") and key.endswith(".failure")
    )
    http_successes = sum(
        value for key, value in metrics_snapshot["counters"].items()
        if key.startswith("http.") and key.endswith(".success")
    )
    total = http_failures + http_successes
    error_rate = http_failures / total if total else 0.0
    endpoint_p95 = max(
        (
            values.get("p95_seconds", 0.0)
            for name, values in metrics_snapshot["timings"].items()
            if name.endswith(".feed") or name.endswith(".comment.page")
        ),
        default=0.0,
    )
    report = {
        "metadata": metadata,
        "http": {
            "successes": http_successes,
            "failures": http_failures,
            "error_rate": round(error_rate, 6),
            "endpoint_p95_seconds": endpoint_p95,
        },
        "threshold_result": {
            "passed": bool(
                metadata.get("status") == "done"
                and metadata.get("note_count", 0) >= metadata.get("target_notes", 0)
                and error_rate < 0.02
                and endpoint_p95 < 1.5
                and metadata.get("validation_after", {}).get("success")
            ),
            "next_note_target": (
                200 if metadata.get("target_notes", 0) < 200
                else min(400, metadata.get("target_notes", 0) + 50)
            ),
            "next_comment_target": (
                2000 if metadata.get("target_comments", 0) < 2000
                else min(4000, metadata.get("target_comments", 0) + 500)
            ),
        },
    }
    Path(path).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


async def main_async(args):
    started_at = datetime.now()
    log_file = setup_run_logging(args.log_dir, args.query, started_at)
    metrics_file = str(Path(log_file).with_suffix(".metrics.json")) if log_file else ""
    calibration_file = str(Path(log_file).with_suffix(".calibration.json")) if log_file else ""
    _, base_path = init()
    cookies_file = resolve_cookies_file(args.cookies_file or None) or ""
    accounts = load_cookie_accounts(cookies_file or None, "")
    if args.account_name:
        accounts = [a for a in accounts if a.name == args.account_name]
        if not accounts:
            raise ValueError(f"没有找到有效账号: {args.account_name}")
    override = args.fixed_concurrency or args.account_concurrency

    metrics = CrawlMetrics()
    from scripts.spider_hangzhou_ecommerce import CookieAccountPool
    sync_pool = CookieAccountPool(accounts, metrics=metrics)
    pool = AsyncAccountPool(
        sync_pool,
        metrics=metrics,
        ignore_quotas=args.calibration_mode,
        runtime_concurrency=override,
    )
    signer = SigningExecutor(args.sign_concurrency, metrics=metrics)
    connector_limit = max(args.note_concurrency + args.comment_concurrency, 20)
    api = AsyncXHSApi(metrics=metrics, signer=signer, connector_limit=connector_limit)
    storage = writer = None
    task_id = None
    run_status = "running"
    run_error = None
    state = None
    validation_before = validation_after = {}
    usage_before = account_snapshot(accounts)
    try:
        if args.use_postgres and not args.dry_run and not args.calibration_mode:
            storage = PostgresStorage()
            storage.init_schema()
            task_id = storage.start_task(args.query, vars(args))
            writer = StorageWriter(
                storage.dsn,
                batch_size=args.db_batch_size,
                flush_seconds=args.db_flush_seconds,
                metrics=metrics,
            ).start()

        if args.calibration_mode:
            validation_before = await verify_account(
                api, accounts[0], "before", metrics, args.query
            )
            if not validation_before["success"]:
                raise RuntimeError(f"标定前账号验证失败: {validation_before['message']}")

        logger.info(
            f"异步流式管线启动: query={args.query}, 目标笔记={args.require_num}, "
            f"评论上限={args.target_comments}, 详情并发={args.note_concurrency}, "
            f"评论并发={args.comment_concurrency}, 账号容量={pool.active_capacity()}, "
            f"签名并发={args.sign_concurrency}"
        )
        state = await run_pipeline(api, pool, args, writer, metrics, storage)
        if writer:
            await asyncio.to_thread(writer.flush)
        if storage:
            storage.finish_task(task_id, "done", state.note_count, state.comment_count)
        if args.save_excel and not args.dry_run and not args.calibration_mode:
            output_dir = build_task_output_dir(base_path, args.query, started_at)
            await asyncio.to_thread(os.makedirs, output_dir, exist_ok=True)
            if storage:
                await asyncio.to_thread(storage.export_to_excel, output_dir, args.query)
            else:
                if state.note_rows:
                    await asyncio.to_thread(
                        save_to_xlsx, state.note_rows,
                        os.path.join(output_dir, f"{args.query}_笔记数据.xlsx"),
                    )
        run_status = "done"
        logger.success(
            f"异步采集完成: 笔记 {state.note_count}，评论 {state.comment_count}，"
            f"停止原因 {state.stop_reason}"
        )
    except Exception as error:
        run_status = "failed"
        run_error = str(error)
        if writer:
            try:
                await asyncio.to_thread(writer.flush)
            except Exception as flush_error:
                logger.error(f"数据库队列清空失败: {flush_error}")
        if storage and task_id:
            storage.finish_task(
                task_id, "failed",
                state.note_count if state else 0,
                state.comment_count if state else 0,
                run_error,
            )
        raise
    finally:
        if args.calibration_mode and accounts:
            validation_after = await verify_account(
                api, accounts[0], "after", metrics, args.query
            )
        if writer:
            await asyncio.to_thread(writer.close)
        await api.close()
        signer.close()
        metrics.log_summary(logger)
        metadata = {
            "query": args.query,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now().isoformat(),
            "status": run_status,
            "error": run_error,
            "task_id": task_id,
            "note_count": state.note_count if state else 0,
            "comment_count": state.comment_count if state else 0,
            "target_notes": args.require_num,
            "target_comments": args.target_comments,
            "stop_reason": state.stop_reason if state else "failed",
            "requested_note_concurrency": args.note_concurrency,
            "comment_concurrency": args.comment_concurrency,
            "active_account_capacity": pool.active_capacity(),
            "runtime_account_concurrency": override,
            "sign_concurrency": args.sign_concurrency,
            "account_names": [a.name for a in accounts],
            "usage_before": usage_before,
            "usage_after": account_snapshot(accounts),
            "validation_before": validation_before,
            "validation_after": validation_after,
            "calibration_mode": args.calibration_mode,
            "metrics_file": metrics_file,
        }
        if metrics_file:
            metrics.write_json(metrics_file, metadata)
        if args.calibration_mode and calibration_file:
            write_calibration_report(calibration_file, metadata, metrics.snapshot())
        if storage:
            storage.close()


def validate_args(args):
    positive = {
        "--require-num": args.require_num,
        "--note-concurrency": args.note_concurrency,
        "--comment-concurrency": args.comment_concurrency,
        "--sign-concurrency": args.sign_concurrency,
        "--max-search-pages": args.max_search_pages,
    }
    for name, value in positive.items():
        if value < 1:
            raise SystemExit(f"{name} 必须大于等于 1")
    if args.target_comments < 0 or args.max_comments_per_note < 0:
        raise SystemExit("评论数量参数必须大于等于 0")
    if args.calibration_mode:
        if not args.account_name:
            raise SystemExit("--calibration-mode 必须同时指定 --account-name")
        args.dry_run = True
        args.use_postgres = False
        args.save_excel = False
        args.ignore_existing_progress = True
    if args.dry_run:
        args.use_postgres = False
        args.save_excel = False
    if not args.dry_run and not args.save_excel and not args.use_postgres:
        raise SystemExit("请启用 --use-postgres、--save-excel 或 --dry-run")


def main(argv=None):
    args = build_parser().parse_args(argv)
    validate_args(args)
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
