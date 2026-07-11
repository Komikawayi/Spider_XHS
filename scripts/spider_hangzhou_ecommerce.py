# -*- coding: utf-8 -*-
"""
杭州电商相关笔记爬虫（含评论采集）
搜索"杭州电商"关键词，采集笔记数据 + 评论数据并保存
"""
import json
import os
import sys
import time
import argparse
import random
import urllib.parse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apis.xhs_pc_apis import XHS_Apis
from xhs_utils.common_util import init
from xhs_utils.data_util import handle_note_info, download_note, save_to_xlsx
from xhs_utils.pg_storage import PostgresStorage


# ========== 常用采集配置 ==========
# 日常使用时优先改这里，然后直接运行：
# python scripts\spider_hangzhou_ecommerce.py
DEFAULT_QUERY = '植村秀'
DEFAULT_REQUIRE_NUM = 1000
DEFAULT_MAX_COMMENTS_PER_NOTE = 500
DEFAULT_DELAY_SECONDS = 1
DEFAULT_NOTE_CONCURRENCY = 8
DEFAULT_SEARCH_EMPTY_RETRIES = 2
DEFAULT_SAVE_EXCEL = True
DEFAULT_USE_POSTGRES = True
DEFAULT_DAILY_NOTE_LIMIT_PER_ACCOUNT = 150
DEFAULT_DAILY_COMMENT_LIMIT_PER_ACCOUNT = 2500
DEFAULT_ACCOUNT_CONCURRENCY_PER_ACCOUNT = 2
DEFAULT_COOKIES_FILE = 'cookies.local.json'
DEFAULT_LOG_DIR = 'logs'
# ==============================


STOP_SIGNAL_WORDS = ('登录', '验证', '验证码', '频繁', '风险', '风控', '限制', '过期', '无权限')
EXPIRED_SIGNAL_WORDS = ('登录', '过期', '无权限')
COOLING_SIGNAL_WORDS = ('验证', '验证码', '频繁', '风险', '风控', '限制')


class StopCrawl(RuntimeError):
    pass


class SearchEmptyResult(RuntimeError):
    pass


class NoAvailableCookieAccounts(RuntimeError):
    pass


@dataclass
class CookieAccount:
    name: str
    cookies: str
    status: str = 'active'
    active_count: int = 0
    max_concurrency: int = DEFAULT_ACCOUNT_CONCURRENCY_PER_ACCOUNT
    daily_note_limit: int = DEFAULT_DAILY_NOTE_LIMIT_PER_ACCOUNT
    daily_comment_limit: int = DEFAULT_DAILY_COMMENT_LIMIT_PER_ACCOUNT
    usage_date: str = ''
    note_ids_today: set = field(default_factory=set)
    comments_today: int = 0
    source_path: str = ''


def today_usage_date():
    return datetime.now().date().isoformat()


def normalize_usage_fields(record):
    today = today_usage_date()
    normalized = dict(record)
    normalized.setdefault('daily_note_limit', DEFAULT_DAILY_NOTE_LIMIT_PER_ACCOUNT)
    normalized.setdefault('daily_comment_limit', DEFAULT_DAILY_COMMENT_LIMIT_PER_ACCOUNT)
    normalized.setdefault('max_concurrency', DEFAULT_ACCOUNT_CONCURRENCY_PER_ACCOUNT)
    if normalized.get('usage_date') != today:
        normalized['usage_date'] = today
        normalized['note_ids_today'] = []
        normalized['comments_today'] = 0
    else:
        normalized.setdefault('note_ids_today', [])
        normalized.setdefault('comments_today', 0)
    return normalized


def cookie_account_from_record(record, index, source_path=''):
    item = normalize_usage_fields(record)
    return CookieAccount(
        str(item.get('name') or f'account_{index + 1}'),
        item.get('cookies', ''),
        status=item.get('status', 'active'),
        daily_note_limit=int(item.get('daily_note_limit') or DEFAULT_DAILY_NOTE_LIMIT_PER_ACCOUNT),
        daily_comment_limit=int(item.get('daily_comment_limit') or DEFAULT_DAILY_COMMENT_LIMIT_PER_ACCOUNT),
        max_concurrency=max(1, int(item.get('max_concurrency') or DEFAULT_ACCOUNT_CONCURRENCY_PER_ACCOUNT)),
        usage_date=item.get('usage_date') or today_usage_date(),
        note_ids_today=set(item.get('note_ids_today') or []),
        comments_today=int(item.get('comments_today') or 0),
        source_path=source_path or '',
    )


def should_stop_for_message(message):
    text = str(message)
    return any(word in text for word in STOP_SIGNAL_WORDS)


def classify_account_failure(message):
    text = str(message)
    if any(word in text for word in EXPIRED_SIGNAL_WORDS):
        return 'expired'
    if any(word in text for word in COOLING_SIGNAL_WORDS):
        return 'cooling'
    return None


def load_cookie_accounts(cookies_file=None, fallback_cookie=None):
    if cookies_file:
        with open(cookies_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        normalized_data = [normalize_usage_fields(item) for item in data]
        if normalized_data != data:
            with open(cookies_file, 'w', encoding='utf-8') as f:
                json.dump(normalized_data, f, ensure_ascii=False, indent=2)
                f.write('\n')
        accounts = []
        for index, item in enumerate(normalized_data):
            if item.get('cookies') and item.get('status', 'active') == 'active':
                accounts.append(cookie_account_from_record(item, index, cookies_file))
        if not accounts:
            raise ValueError(f'Cookie 文件没有可用账号: {cookies_file}')
        return accounts
    if fallback_cookie:
        return [CookieAccount('default', fallback_cookie)]
    raise ValueError('未找到可用 Cookie。请配置 .env COOKIES 或传入 --cookies-file')


def resolve_cookies_file(cookies_file='', cwd=None):
    if cookies_file:
        return cookies_file
    local_path = Path(cwd or os.getcwd()) / DEFAULT_COOKIES_FILE
    if local_path.exists():
        return str(local_path)
    return None


def setup_run_logging(log_dir, query, started_at):
    if not log_dir:
        return ''
    safe_query = ''.join(ch for ch in query if ch not in r'\/:*?"<>| ').strip() or 'xhs'
    path = Path(log_dir) / f'xhs_crawl_{safe_query}_{started_at.strftime("%Y%m%d_%H%M%S")}.log'
    path.parent.mkdir(parents=True, exist_ok=True)
    logger.add(str(path), encoding='utf-8', enqueue=True, rotation='20 MB', backtrace=True, diagnose=False)
    return str(path)


class CookieAccountPool:
    def __init__(self, accounts):
        if not accounts:
            raise ValueError('账号池不能为空')
        self.accounts = accounts
        self._condition = threading.Condition()

    def acquire(self):
        return self._acquire_matching(
            lambda account: True,
            '没有可用账号：所有 Cookie 均已过期或进入冷却',
        )

    def acquire_for_note(self, note_id):
        return self._acquire_matching(
            lambda account: note_id in account.note_ids_today or len(account.note_ids_today) < account.daily_note_limit,
            '没有可用账号：所有 Cookie 均已过期、进入冷却或达到每日图文额度',
        )

    def acquire_for_comments(self):
        return self._acquire_matching(
            lambda account: account.comments_today < account.daily_comment_limit,
            '没有可用账号：所有 Cookie 均已过期、进入冷却或达到每日评论额度',
        )

    def _acquire_matching(self, predicate, exhausted_message):
        with self._condition:
            while True:
                for account in self.accounts:
                    if (
                        account.status == 'active'
                        and account.active_count < account.max_concurrency
                        and predicate(account)
                    ):
                        account.active_count += 1
                        return account
                if any(account.status == 'active' and account.active_count > 0 and predicate(account) for account in self.accounts):
                    self._condition.wait()
                    continue
                raise NoAvailableCookieAccounts(exhausted_message)

    def report_success(self, account):
        with self._condition:
            account.active_count = max(0, account.active_count - 1)
            self._persist_usage(account)
            self._condition.notify_all()

    def report_note_success(self, account, note_id):
        with self._condition:
            account.note_ids_today.add(note_id)
            account.active_count = max(0, account.active_count - 1)
            self._persist_usage(account)
            self._condition.notify_all()

    def report_comments_success(self, account, comment_count):
        with self._condition:
            account.comments_today += max(0, int(comment_count or 0))
            account.active_count = max(0, account.active_count - 1)
            self._persist_usage(account)
            self._condition.notify_all()

    def report_failure(self, account, message):
        status = classify_account_failure(message) or 'cooling'
        with self._condition:
            account.status = status
            account.active_count = max(0, account.active_count - 1)
            self._persist_usage(account)
            self._condition.notify_all()

    def _persist_usage(self, account):
        if not account.source_path:
            return
        path = Path(account.source_path)
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
        except Exception as e:
            logger.warning(f'账号用量写回失败: {e}')
            return
        changed = False
        for item in data:
            if item.get('name') == account.name:
                item['status'] = account.status
                item['daily_note_limit'] = account.daily_note_limit
                item['daily_comment_limit'] = account.daily_comment_limit
                item['max_concurrency'] = account.max_concurrency
                item['usage_date'] = account.usage_date or today_usage_date()
                item['note_ids_today'] = sorted(account.note_ids_today)
                item['comments_today'] = account.comments_today
                changed = True
                break
        if changed:
            tmp_path = path.with_name(f'{path.name}.{os.getpid()}.{threading.get_ident()}.tmp')
            tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
            os.replace(tmp_path, path)


def ensure_cookie_pool(cookie_source):
    if isinstance(cookie_source, CookieAccountPool):
        return cookie_source
    return CookieAccountPool([CookieAccount('default', cookie_source)])


def save_comments_to_xlsx(comments_data, file_path):
    """保存评论数据到Excel"""
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "评论数据"

    # 写入表头
    headers = [
        '笔记ID', '笔记标题', '评论ID', '评论内容', '评论用户昵称',
        '评论用户ID', '点赞数', '评论时间', 'IP归属地', '是否热评',
        '子评论数量', '子评论内容'
    ]
    ws.append(headers)

    # 写入数据
    for item in comments_data:
        # 主评论
        row = [
            item.get('note_id', ''),
            item.get('note_title', ''),
            item.get('comment_id', ''),
            item.get('content', ''),
            item.get('user_nickname', ''),
            item.get('user_id', ''),
            item.get('like_count', 0),
            item.get('create_time', ''),
            item.get('ip_location', ''),
            item.get('is_hot', False),
            item.get('sub_comment_count', 0),
            ''  # 子评论单独处理
        ]
        ws.append(row)

        # 子评论
        for sub in item.get('sub_comments', []):
            sub_row = [
                item.get('note_id', ''),
                item.get('note_title', ''),
                sub.get('comment_id', ''),
                sub.get('content', ''),
                sub.get('user_nickname', ''),
                sub.get('user_id', ''),
                sub.get('like_count', 0),
                sub.get('create_time', ''),
                sub.get('ip_location', ''),
                False,
                0,
                f'回复: {sub.get("reply_to", "")}'
            ]
            ws.append(sub_row)

    # 设置列宽
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = min(max_length + 2, 50)
        ws.column_dimensions[column].width = adjusted_width

    wb.save(file_path)


def parse_comment(comment, note_id, note_title):
    """解析单条评论数据"""
    user_info = comment.get('user_info', {})
    create_time = comment.get('create_time', 0)
    if create_time:
        create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(create_time / 1000))

    return {
        'note_id': note_id,
        'note_title': note_title,
        'comment_id': comment.get('id', ''),
        'content': comment.get('content', ''),
        'user_nickname': user_info.get('nickname', ''),
        'user_id': user_info.get('user_id', ''),
        'like_count': comment.get('like_count', 0),
        'create_time': create_time,
        'ip_location': comment.get('ip_location', ''),
        'is_hot': comment.get('is_hot', False),
        'sub_comment_count': comment.get('sub_comment_count', 0),
        'sub_comments': []
    }


def build_note_url(note):
    params = {}
    xsec_token = note.get('xsec_token', '')
    if xsec_token:
        params['xsec_token'] = xsec_token
    query = urllib.parse.urlencode(params)
    suffix = f'?{query}' if query else ''
    return f"https://www.xiaohongshu.com/explore/{note['id']}{suffix}"


def iter_image_notes(notes):
    seen = set()
    for note in notes:
        note_id = note.get('id')
        if not note_id or note_id in seen:
            continue
        seen.add(note_id)
        if note.get('model_type') != 'note':
            continue
        if note.get('note_card', {}).get('type', 'normal') == 'video':
            continue
        yield note


def filter_notes_for_collection(notes, storage, max_comments_per_note):
    if not storage:
        return list(notes)
    note_ids = [note['id'] for note in notes if note.get('id')]
    progress_by_id = storage.get_comment_progress_by_note_ids(note_ids)
    filtered = []
    for note in notes:
        note_id = note.get('id')
        progress = progress_by_id.get(note_id)
        if progress:
            is_done = progress.get('comments_crawl_status') == 'done'
            target_met = int(progress.get('comments_target_count') or 0) >= max_comments_per_note
            no_more_comments = progress.get('comments_has_more') is False
            if is_done and (target_met or no_more_comments):
                logger.info(f'跳过已完成笔记: {note_id}')
                continue
            note = dict(note)
            note['_comments_next_cursor'] = progress.get('comments_next_cursor') or ''
            note['_comments_collected_count'] = int(progress.get('comments_collected_count') or 0)
        filtered.append(note)
    return filtered


def sleep_between_requests(delay_seconds):
    if delay_seconds <= 0:
        return
    time.sleep(delay_seconds + random.uniform(0, delay_seconds * 0.4))


def parse_note_url_context(note_url):
    url_parse = urllib.parse.urlparse(note_url)
    note_id = url_parse.path.split('/')[-1]
    kv_dist = {
        key: values[-1] if values else ''
        for key, values in urllib.parse.parse_qs(url_parse.query, keep_blank_values=True).items()
    }
    return note_id, kv_dist.get('xsec_token', '')


def fetch_comment_page(xhs_apis, note_url, cookies_str, cursor='', proxies=None):
    note_id, xsec_token = parse_note_url_context(note_url)
    success, msg, res_json = xhs_apis.get_note_out_comment(note_id, cursor, xsec_token, cookies_str, proxies)
    if not success:
        if should_stop_for_message(msg):
            raise StopCrawl(f'触发停止信号: {msg}')
        raise RuntimeError(msg)
    data = res_json.get('data', {})
    return data.get('comments', []), str(data.get('cursor') or ''), bool(data.get('has_more'))


def collect_comments_limited(xhs_apis, note_url, cookies_str, max_comments, delay_seconds=0, proxies=None, start_cursor=''):
    cursor = start_cursor
    comments = []
    while len(comments) < max_comments:
        page_comments, next_cursor, has_more = fetch_comment_page(xhs_apis, note_url, cookies_str, cursor, proxies)
        for comment in page_comments:
            comments.append(comment)
            if len(comments) >= max_comments:
                break
        if len(comments) >= max_comments or not has_more or not next_cursor:
            break
        cursor = next_cursor
        sleep_between_requests(delay_seconds)
    return comments


def search_notes_with_retry(
    xhs_apis, query, require_num, cookies_str,
    sort_type_choice, note_type, note_time, note_range,
    retries=DEFAULT_SEARCH_EMPTY_RETRIES,
    retry_delay_seconds=DEFAULT_DELAY_SECONDS,
):
    last_msg = ''
    for attempt in range(retries + 1):
        success, msg, notes = xhs_apis.search_some_note(
            query, require_num, cookies_str,
            sort_type_choice, note_type, note_time, note_range
        )
        last_msg = msg
        if not success:
            return success, msg, notes
        if notes:
            return success, msg, notes
        if attempt < retries:
            logger.warning(f'搜索返回空结果，{retry_delay_seconds} 秒后重试 ({attempt + 1}/{retries})')
            sleep_between_requests(retry_delay_seconds)
    raise SearchEmptyResult(f'搜索连续返回空结果：{query}。建议换账号或稍后重试。最后消息: {last_msg}')


def search_notes_with_cookie_pool(
    xhs_apis, query, require_num, cookie_pool,
    sort_type_choice, note_type, note_time, note_range,
):
    while True:
        account = cookie_pool.acquire()
        try:
            success, msg, notes = search_notes_with_retry(
                xhs_apis, query, require_num, account.cookies,
                sort_type_choice, note_type, note_time, note_range
            )
        except StopCrawl as e:
            cookie_pool.report_failure(account, e)
            logger.warning(f'搜索账号 {account.name} 不可用: {e}')
            continue
        except SearchEmptyResult as e:
            cookie_pool.report_failure(account, e)
            logger.warning(f'搜索账号 {account.name} 连续空结果，换下一个账号: {e}')
            continue
        except Exception:
            cookie_pool.report_success(account)
            raise
        if success:
            cookie_pool.report_success(account)
            return success, msg, notes
        if should_stop_for_message(msg):
            cookie_pool.report_failure(account, msg)
            logger.warning(f'搜索账号 {account.name} 不可用: {msg}')
            continue
        cookie_pool.report_success(account)
        return success, msg, notes


def collect_note_result(
    index, total, note, xhs_apis, cookie_source,
    max_comments_per_note, delay_seconds, base_path,
    download_media_flag=False, storage=None, keyword='', storage_lock=None,
):
    cookie_pool = ensure_cookie_pool(cookie_source)
    storage_lock = storage_lock or threading.Lock()
    note_id = note['id']
    note_url = build_note_url(note)

    logger.info(f'[{index+1}/{total}] 爬取图文笔记: {note_url}')

    while True:
        account = cookie_pool.acquire_for_note(note_id)
        try:
            success, msg, note_info = xhs_apis.get_note_info(note_url, account.cookies)
        except Exception as e:
            if should_stop_for_message(e):
                cookie_pool.report_failure(account, e)
                logger.warning(f'  账号 {account.name} 获取详情受限: {e}')
                continue
            cookie_pool.report_success(account)
            logger.error(f'  笔记详情异常: {e}')
            return None
        if success and note_info:
            cookie_pool.report_note_success(account, note_id)
            break
        if should_stop_for_message(msg):
            cookie_pool.report_failure(account, msg)
            logger.warning(f'  账号 {account.name} 获取详情受限: {msg}')
            continue
        cookie_pool.report_success(account)
        logger.warning(f'  笔记详情获取失败: {msg}')
        return None

    items = (note_info.get('data') or {}).get('items') or []
    if not items:
        logger.warning(f'  笔记详情响应缺少 items，跳过: {note_id}')
        return None
    raw_note = items[0]
    raw_note['url'] = note_url
    note_data = handle_note_info(raw_note)
    if note_data.get('note_type') != '图集':
        logger.info(f'  跳过非图文笔记: {note_id}')
        return None
    note_title = note_data.get('title', '无标题')
    logger.info(f'  标题: {note_title[:50]}')
    if storage:
        with storage_lock:
            storage.upsert_note(note_data, keyword=keyword, raw=raw_note)
    if download_media_flag:
        download_note(note_data, base_path['media'], 'media-image')

    sleep_between_requests(delay_seconds)

    comment_rows = []
    logger.info(f'  开始采集评论，最多 {max_comments_per_note} 条...')

    cursor = note.get('_comments_next_cursor', '')
    if storage:
        with storage_lock:
            collected_count = storage.count_comments(note_id)
    else:
        collected_count = int(note.get('_comments_collected_count') or 0)

    while collected_count < max_comments_per_note:
        account = cookie_pool.acquire_for_comments()
        try:
            page_comments, next_cursor, has_more = fetch_comment_page(
                xhs_apis, note_url, account.cookies, cursor
            )
        except StopCrawl as e:
            cookie_pool.report_failure(account, e)
            logger.warning(f'  账号 {account.name} 评论采集受限: {e}')
            if storage:
                with storage_lock:
                    storage.update_comment_progress(
                        note_id, 'partial', collected_count, max_comments_per_note, cursor, True
                    )
            continue
        except Exception as e:
            cookie_pool.report_success(account)
            logger.error(f'  评论采集异常: {e}')
            if storage:
                with storage_lock:
                    storage.update_comment_progress(
                        note_id, 'partial', collected_count, max_comments_per_note, cursor, True
                    )
            break

        remaining = max_comments_per_note - collected_count
        saved_comment_count = min(len(page_comments), remaining)
        for comment in page_comments[:remaining]:
            comment_data = parse_comment(comment, note_id, note_title)
            comment_rows.append((comment_data, comment))
            if storage:
                with storage_lock:
                    storage.upsert_comment(comment_data, note_id, note_title, raw=comment)
        cookie_pool.report_comments_success(account, saved_comment_count)

        if storage:
            with storage_lock:
                collected_count = storage.count_comments(note_id)
        else:
            collected_count += min(len(page_comments), remaining)

        is_done = collected_count >= max_comments_per_note or not has_more or not next_cursor
        status = 'done' if is_done else 'partial'
        progress_cursor = next_cursor if has_more and next_cursor else ''
        if storage:
            with storage_lock:
                storage.update_comment_progress(
                    note_id, status, collected_count, max_comments_per_note,
                    progress_cursor, has_more
                )
        if is_done:
            break
        cursor = next_cursor
        sleep_between_requests(delay_seconds)
    if max_comments_per_note <= 0 and storage:
        with storage_lock:
            storage.update_comment_progress(note_id, 'done', collected_count, max_comments_per_note, '', False)
    logger.info(f'  采集到 {len(comment_rows)} 条一级评论')

    sleep_between_requests(delay_seconds)

    return {
        'note_data': note_data,
        'raw_note': raw_note,
        'comments': comment_rows,
        'stored': bool(storage),
    }


def collect_note_results(
    notes, xhs_apis, cookies_str, max_comments_per_note,
    delay_seconds, base_path, download_media_flag, note_concurrency,
    storage=None, keyword='', storage_lock=None,
):
    total = len(notes)
    cookie_pool = ensure_cookie_pool(cookies_str)
    storage_lock = storage_lock or threading.Lock()
    if note_concurrency <= 1:
        for i, note in enumerate(notes):
            yield collect_note_result(
                i, total, note, xhs_apis, cookie_pool,
                max_comments_per_note, delay_seconds, base_path,
                download_media_flag, storage, keyword, storage_lock
            )
        return

    with ThreadPoolExecutor(max_workers=note_concurrency) as executor:
        futures = [
            executor.submit(
                collect_note_result,
                i, total, note, xhs_apis, cookie_pool,
                max_comments_per_note, delay_seconds, base_path,
                download_media_flag, storage, keyword, storage_lock
            )
            for i, note in enumerate(notes)
        ]
        for future in as_completed(futures):
            yield future.result()


def collect_notes(
    notes, xhs_apis, cookies_str, max_comments_per_note,
    delay_seconds, base_path, download_media_flag, note_concurrency,
):
    results = collect_note_results(
        notes, xhs_apis, cookies_str, max_comments_per_note,
        delay_seconds, base_path, download_media_flag, note_concurrency
    )
    note_list = []
    all_comments = []
    for result in results:
        if not result:
            continue
        note_list.append(result['note_data'])
        all_comments.extend(comment_data for comment_data, _ in result['comments'])
    return note_list, all_comments


def build_parser():
    parser = argparse.ArgumentParser(description='采集杭州电商图文笔记，并保存到 PostgreSQL / Excel')
    parser.add_argument('--query', default=DEFAULT_QUERY)
    parser.add_argument('--require-num', type=int, default=DEFAULT_REQUIRE_NUM)
    parser.add_argument('--max-comments-per-note', type=int, default=DEFAULT_MAX_COMMENTS_PER_NOTE)
    parser.add_argument('--delay-seconds', type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument('--note-concurrency', type=int, default=DEFAULT_NOTE_CONCURRENCY)
    parser.add_argument('--account-concurrency', type=int, default=None, help='覆盖每个账号同时允许的并发槽数')
    parser.add_argument('--cookies-file', default='')
    parser.add_argument('--log-dir', default=DEFAULT_LOG_DIR)
    parser.add_argument('--sort-type-choice', type=int, default=0)
    parser.add_argument('--note-time', type=int, default=0)
    parser.add_argument('--note-range', type=int, default=0)
    parser.add_argument('--save-excel', dest='save_excel', action='store_true', default=DEFAULT_SAVE_EXCEL)
    parser.add_argument('--no-excel', dest='save_excel', action='store_false')
    parser.add_argument('--use-postgres', dest='use_postgres', action='store_true', default=DEFAULT_USE_POSTGRES)
    parser.add_argument('--no-postgres', dest='use_postgres', action='store_false')
    parser.add_argument('--download-media', action='store_true')
    parser.add_argument('--dry-run', action='store_true', help='只测试采集流程，不保存 PostgreSQL / Excel')
    return parser


def validate_args(args):
    if args.dry_run:
        args.use_postgres = False
        args.save_excel = False
    if args.note_concurrency < 1:
        raise SystemExit('--note-concurrency 必须大于等于 1')
    if args.account_concurrency is not None and args.account_concurrency < 1:
        raise SystemExit('--account-concurrency 必须大于等于 1')
    if not args.use_postgres and not args.save_excel and not args.dry_run:
        raise SystemExit(
            '当前命令不会保存任何数据。请添加 --use-postgres 保存到数据库，'
            '或添加 --save-excel 保存到 Excel；如果只是试跑，请显式添加 --dry-run。'
        )


def build_task_output_dir(base_path, query, started_at):
    safe_query = ''.join(ch for ch in query if ch not in r'\/:*?"<>| ').strip() or 'xhs'
    folder_name = f'{safe_query}_{started_at.strftime("%Y%m%d_%H%M%S")}'
    return str(Path(base_path['excel']) / folder_name)


def main(argv=None):
    args = build_parser().parse_args(argv)
    validate_args(args)
    started_at = datetime.now()
    log_file = setup_run_logging(args.log_dir, args.query, started_at)
    cookies_str, base_path = init()
    args.cookies_file = resolve_cookies_file(args.cookies_file or None) or ''
    accounts = load_cookie_accounts(args.cookies_file or None, cookies_str)
    if args.account_concurrency is not None:
        for account in accounts:
            account.max_concurrency = args.account_concurrency
    cookie_pool = CookieAccountPool(accounts)
    xhs_apis = XHS_Apis()
    storage = None
    task_id = None
    note_count = 0
    comment_count = 0

    # ========== 搜索配置 ==========
    query = args.query
    require_num = args.require_num
    max_comments_per_note = args.max_comments_per_note
    sort_type_choice = args.sort_type_choice
    note_type = 2  # 只采普通图文笔记
    note_time = args.note_time
    note_range = args.note_range
    # ==============================

    task_excel_dir = build_task_output_dir(base_path, query, started_at)

    if log_file:
        logger.info(f'实时日志文件: {log_file}')
    if args.cookies_file:
        logger.info(
            f'使用 Cookie 文件: {args.cookies_file}，可用账号: '
            f'{", ".join(f"{account.name}(并发{account.max_concurrency})" for account in accounts)}'
        )
    else:
        logger.info('使用 .env COOKIES 单账号模式')
    logger.info(f'开始搜索: "{query}", 目标笔记数: {require_num}')
    if args.save_excel:
        logger.info(f'本次 Excel 将保存到: {task_excel_dir}')
    if args.use_postgres:
        storage = PostgresStorage()
        storage.init_schema()
        task_id = storage.start_task(query, vars(args))

    try:
        # 1. 搜索图文笔记
        success, msg, notes = search_notes_with_cookie_pool(
            xhs_apis,
            query, require_num, cookie_pool,
            sort_type_choice, note_type, note_time, note_range
        )

        if not success:
            if should_stop_for_message(msg):
                raise StopCrawl(f'触发停止信号: {msg}')
            raise RuntimeError(f'搜索失败: {msg}')

        notes = list(iter_image_notes(notes))
        notes = filter_notes_for_collection(notes, storage, max_comments_per_note)
        logger.info(f'搜索完成，去重后获取到 {len(notes)} 条图文笔记')

        # 2. 逐条爬取笔记详情 + 限量评论
        note_list = []
        all_comments = []

        results = collect_note_results(
            notes, xhs_apis, cookie_pool, max_comments_per_note,
            args.delay_seconds, base_path, args.download_media,
            args.note_concurrency, storage, query
        )
        for result in results:
            if not result:
                continue
            note_data = result['note_data']
            note_list.append(note_data)
            note_count += 1
            if storage and not result.get('stored'):
                storage.upsert_note(note_data, keyword=query, raw=result['raw_note'])
            for comment_data, raw_comment in result['comments']:
                all_comments.append(comment_data)
                comment_count += 1
                if storage and not result.get('stored'):
                    storage.upsert_comment(
                        comment_data,
                        note_data['note_id'],
                        note_data.get('title', '无标题'),
                        raw=raw_comment
                    )

        # 3. 保存 Excel
        if args.save_excel:
            excel_dir = task_excel_dir
            os.makedirs(excel_dir, exist_ok=True)

            if note_list:
                note_excel_path = os.path.abspath(os.path.join(excel_dir, f'{query}_笔记数据.xlsx'))
                save_to_xlsx(note_list, note_excel_path)
                logger.info(f'笔记 Excel 已保存: {note_excel_path}')

            if all_comments:
                comments_excel_path = os.path.abspath(os.path.join(excel_dir, f'{query}_评论数据.xlsx'))
                save_comments_to_xlsx(all_comments, comments_excel_path)
                logger.info(f'评论 Excel 已保存: {comments_excel_path}')
                logger.info(f'共采集 {len(all_comments)} 条一级评论')

        if storage:
            storage.finish_task(task_id, 'done', note_count, comment_count)

        # 4. 统计
        logger.info('=' * 50)
        logger.info(f'采集完成统计:')
        logger.info(f'  笔记数量: {note_count}')
        logger.info(f'  一级评论: {comment_count}')
        logger.info('=' * 50)
    except Exception as e:
        if storage and task_id:
            storage.finish_task(task_id, 'failed', note_count, comment_count, str(e))
        logger.error(str(e))
        raise
    finally:
        if storage:
            storage.close()


if __name__ == '__main__':
    main()
