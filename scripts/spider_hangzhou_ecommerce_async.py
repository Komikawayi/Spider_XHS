# -*- coding: utf-8 -*-
"""aiohttp + asyncio 采集入口；保留线程版脚本作为稳定回退。"""

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apis.xhs_pc_async_apis import AsyncXHSApi
from scripts.spider_hangzhou_ecommerce import (
    CookieAccountPool,
    NoAvailableCookieAccounts,
    StopCrawl,
    build_note_url,
    build_task_output_dir,
    iter_image_notes,
    load_cookie_accounts,
    parse_comment,
    parse_note_url_context,
    resolve_cookies_file,
    save_comments_to_xlsx,
    setup_run_logging,
    should_stop_for_message,
)
from xhs_utils.common_util import init
from xhs_utils.crawl_metrics import CrawlMetrics
from xhs_utils.data_util import handle_note_info, save_to_xlsx
from xhs_utils.pg_storage import PostgresStorage
from xhs_utils.signing_executor import SigningExecutor
from xhs_utils.storage_writer import StorageWriter


DEFAULT_QUERY = "植村秀"
DEFAULT_REQUIRE_NUM = 100
DEFAULT_MAX_COMMENTS_PER_NOTE = 100
DEFAULT_NOTE_CONCURRENCY = 10
DEFAULT_SIGN_CONCURRENCY = 4
DEFAULT_DB_BATCH_SIZE = 100
DEFAULT_DB_FLUSH_SECONDS = 0.05
DEFAULT_LOG_DIR = "logs"


class AsyncAccountPool:
    """把已验证的线程安全账号池适配给 asyncio，不阻塞事件循环。"""

    def __init__(self, pool):
        self._pool = pool

    async def acquire(self):
        return await asyncio.to_thread(self._pool.acquire)

    async def acquire_for_note(self, note_id):
        return await asyncio.to_thread(self._pool.acquire_for_note, note_id)

    async def acquire_for_comments(self):
        return await asyncio.to_thread(self._pool.acquire_for_comments)

    async def report_success(self, account):
        await asyncio.to_thread(self._pool.report_success, account)

    async def report_note_success(self, account, note_id):
        await asyncio.to_thread(self._pool.report_note_success, account, note_id)

    async def report_comments_success(self, account, count):
        await asyncio.to_thread(self._pool.report_comments_success, account, count)

    async def report_failure(self, account, message):
        await asyncio.to_thread(self._pool.report_failure, account, message)


async def wait_writer(future):
    return await asyncio.wrap_future(future)


async def search_notes(api, pool, query, require_num):
    account = await pool.acquire()
    released = False
    try:
        notes = []
        page = 1
        while len(notes) < require_num:
            success, message, payload = await api.search_note(query, account.cookies, page=page)
            if not success:
                if should_stop_for_message(message):
                    await pool.report_failure(account, message)
                    released = True
                    raise StopCrawl(message)
                raise RuntimeError(f"搜索失败: {message}")
            data = (payload or {}).get("data") or {}
            page_notes = data.get("items") or []
            notes.extend(page_notes)
            if not data.get("has_more") or not page_notes:
                break
            page += 1
        await pool.report_success(account)
        released = True
        return notes[:require_num]
    except Exception:
        if not released:
            await pool.report_success(account)
        raise


async def collect_comments(api, pool, note_url, note_id, note_title, limit, writer, metrics):
    if limit <= 0:
        if writer:
            future = await asyncio.to_thread(
                writer.submit_progress, note_id, "done", 0, limit, "", False
            )
            await wait_writer(future)
        return []

    _, xsec_token = parse_note_url_context(note_url)
    cursor = ""
    collected = 0
    rows = []
    while collected < limit:
        account = await pool.acquire_for_comments()
        released = False
        try:
            success, message, payload = await api.get_note_out_comment(
                note_id, cursor, xsec_token, account.cookies
            )
            if not success:
                if should_stop_for_message(message):
                    await pool.report_failure(account, message)
                    released = True
                    continue
                await pool.report_success(account)
                released = True
                raise RuntimeError(message)
            data = (payload or {}).get("data") or {}
            comments = data.get("comments") or []
            next_cursor = str(data.get("cursor") or "")
            has_more = bool(data.get("has_more"))
            remaining = limit - collected
            page_rows = [
                (parse_comment(comment, note_id, note_title), comment)
                for comment in comments[:remaining]
            ]
            await pool.report_comments_success(account, len(page_rows))
            released = True
        except Exception:
            if not released:
                await pool.report_success(account)
            raise

        rows.extend(page_rows)
        if writer:
            future = await asyncio.to_thread(
                writer.submit_comment_page,
                note_id,
                note_title,
                page_rows,
                limit,
                next_cursor,
                has_more,
            )
            state = await wait_writer(future)
            collected = state["collected_count"]
            done = state["is_done"]
        else:
            collected += len(page_rows)
            done = collected >= limit or not has_more or not next_cursor
        if metrics:
            metrics.increment("comments.fetched", len(page_rows))
        if done:
            break
        cursor = next_cursor
    return rows


async def collect_note(api, pool, note, max_comments, writer, metrics, keyword):
    note_id = note["id"]
    note_url = build_note_url(note)
    if metrics:
        metrics.increment("notes.started")
    while True:
        account = await pool.acquire_for_note(note_id)
        released = False
        try:
            success, message, payload = await api.get_note_info(note_url, account.cookies)
        except Exception as error:
            if should_stop_for_message(error):
                await pool.report_failure(account, error)
                released = True
                continue
            await pool.report_success(account)
            released = True
            raise
        if success and payload:
            await pool.report_note_success(account, note_id)
            released = True
            break
        if should_stop_for_message(message):
            await pool.report_failure(account, message)
            released = True
            continue
        await pool.report_success(account)
        released = True
        raise RuntimeError(message)

    items = ((payload.get("data") or {}).get("items")) or []
    if not items:
        if metrics:
            metrics.increment("notes.skipped")
        return None
    raw_note = items[0]
    raw_note["url"] = note_url
    note_data = handle_note_info(raw_note)
    if note_data.get("note_type") != "图集":
        if metrics:
            metrics.increment("notes.skipped")
        return None
    if writer:
        await asyncio.to_thread(writer.submit_note, note_data, keyword, raw_note)
    comments = await collect_comments(
        api, pool, note_url, note_id, note_data.get("title", "无标题"),
        max_comments, writer, metrics,
    )
    if metrics:
        metrics.increment("notes.completed")
    return {"note_data": note_data, "comments": comments}


async def run_workers(api, pool, notes, args, writer, metrics):
    worker_count = max(1, min(args.note_concurrency, pool._pool.active_capacity() or 1))
    metrics.observe_max("scheduler.async_note_workers", worker_count)
    queue = asyncio.Queue(maxsize=worker_count * 2)
    results = []

    async def worker():
        while True:
            note = await queue.get()
            try:
                if note is None:
                    return
                result = await collect_note(
                    api, pool, note, args.max_comments_per_note, writer, metrics, args.query
                )
                if result:
                    results.append(result)
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
    for note in notes:
        await queue.put(note)
    for _ in workers:
        await queue.put(None)
    await queue.join()
    await asyncio.gather(*workers)
    return results


def build_parser():
    parser = argparse.ArgumentParser(description="小红书 aiohttp + asyncio 采集管线")
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--require-num", type=int, default=DEFAULT_REQUIRE_NUM)
    parser.add_argument("--max-comments-per-note", type=int, default=DEFAULT_MAX_COMMENTS_PER_NOTE)
    parser.add_argument("--note-concurrency", type=int, default=DEFAULT_NOTE_CONCURRENCY)
    parser.add_argument("--account-concurrency", type=int, default=None)
    parser.add_argument("--sign-concurrency", type=int, default=DEFAULT_SIGN_CONCURRENCY)
    parser.add_argument("--db-batch-size", type=int, default=DEFAULT_DB_BATCH_SIZE)
    parser.add_argument("--db-flush-seconds", type=float, default=DEFAULT_DB_FLUSH_SECONDS)
    parser.add_argument("--cookies-file", default="")
    parser.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
    parser.add_argument("--save-excel", action="store_true")
    parser.add_argument("--use-postgres", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


async def main_async(args):
    started_at = datetime.now()
    log_file = setup_run_logging(args.log_dir, args.query, started_at)
    metrics_file = str(Path(log_file).with_suffix(".metrics.json")) if log_file else ""
    _, base_path = init()
    cookies_file = resolve_cookies_file(args.cookies_file or None) or ""
    accounts = load_cookie_accounts(cookies_file or None, "")
    if args.account_concurrency is not None:
        for account in accounts:
            account.max_concurrency = args.account_concurrency
    metrics = CrawlMetrics()
    sync_pool = CookieAccountPool(accounts, metrics=metrics)
    pool = AsyncAccountPool(sync_pool)
    signer = SigningExecutor(args.sign_concurrency, metrics=metrics)
    api = AsyncXHSApi(metrics=metrics, signer=signer, connector_limit=args.note_concurrency)
    storage = writer = None
    task_id = None
    run_status = "running"
    run_error = None
    note_count = comment_count = 0
    try:
        if args.use_postgres and not args.dry_run:
            storage = PostgresStorage()
            storage.init_schema()
            task_id = storage.start_task(args.query, vars(args))
            writer = StorageWriter(
                storage.dsn,
                batch_size=args.db_batch_size,
                flush_seconds=args.db_flush_seconds,
                metrics=metrics,
            ).start()

        logger.info(
            f"异步管线启动: query={args.query}, 并发={args.note_concurrency}, "
            f"账号容量={sync_pool.active_capacity()}, 签名并发={args.sign_concurrency}"
        )
        notes = await search_notes(api, pool, args.query, args.require_num)
        notes = list(iter_image_notes(notes))
        results = await run_workers(api, pool, notes, args, writer, metrics)
        note_list = [result["note_data"] for result in results]
        comments = [row[0] for result in results for row in result["comments"]]
        note_count = len(note_list)
        comment_count = len(comments)

        if args.save_excel and not args.dry_run:
            output_dir = build_task_output_dir(base_path, args.query, started_at)
            await asyncio.to_thread(os.makedirs, output_dir, exist_ok=True)
            if note_list:
                await asyncio.to_thread(
                    save_to_xlsx, note_list, os.path.join(output_dir, f"{args.query}_笔记数据.xlsx")
                )
            if comments:
                await asyncio.to_thread(
                    save_comments_to_xlsx, comments,
                    os.path.join(output_dir, f"{args.query}_评论数据.xlsx"),
                )
        if writer:
            await asyncio.to_thread(writer.flush)
        if storage:
            storage.finish_task(task_id, "done", note_count, comment_count)
        run_status = "done"
        logger.success(f"异步采集完成: 笔记 {note_count}，评论 {comment_count}")
    except Exception as error:
        run_status = "failed"
        run_error = str(error)
        if writer:
            try:
                await asyncio.to_thread(writer.flush)
            except Exception as flush_error:
                logger.error(f"数据库队列清空失败: {flush_error}")
        if storage and task_id:
            storage.finish_task(task_id, "failed", note_count, comment_count, run_error)
        raise
    finally:
        if writer:
            await asyncio.to_thread(writer.close)
        await api.close()
        signer.close()
        metrics.log_summary(logger)
        if metrics_file:
            metrics.write_json(metrics_file, {
                "query": args.query,
                "started_at": started_at.isoformat(),
                "finished_at": datetime.now().isoformat(),
                "status": run_status,
                "error": run_error,
                "note_count": note_count,
                "comment_count": comment_count,
                "requested_note_concurrency": args.note_concurrency,
                "active_account_capacity": sync_pool.active_capacity(),
                "sign_concurrency": args.sign_concurrency,
            })
        if storage:
            storage.close()


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.note_concurrency < 1 or args.sign_concurrency < 1:
        raise SystemExit("并发参数必须大于等于 1")
    if args.dry_run:
        args.use_postgres = False
        args.save_excel = False
    if not args.dry_run and not args.save_excel and not args.use_postgres:
        raise SystemExit("请启用 --use-postgres、--save-excel 或 --dry-run")
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
