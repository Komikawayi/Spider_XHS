# -*- coding: utf-8 -*-
"""
小红书 PC 端 Cookie 管理脚本。

扫码登录逻辑直接复用仓库已有的 apis.xhs_pc_login_apis.XHSLoginApi.qrcode_login，
这里只负责批量调用并保存到 cookies.local.json。
"""
import argparse
import getpass
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apis.xhs_pc_login_apis import XHSLoginApi
from apis.xhs_pc_apis import XHS_Apis
from xhs_utils.cookie_util import trans_cookies


DEFAULT_OUTPUT = "cookies.local.json"
DEFAULT_DAILY_NOTE_LIMIT_PER_ACCOUNT = 150
DEFAULT_DAILY_COMMENT_LIMIT_PER_ACCOUNT = 2500
DEFAULT_ACCOUNT_CONCURRENCY_PER_ACCOUNT = 10


def load_cookie_records(path):
    path = Path(path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Cookie 文件必须是 JSON 数组: {path}")
    return data


def save_cookie_records(path, records):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
        f.write("\n")


def next_account_name(records, prefix="account"):
    existing = {record.get("name") for record in records}
    index = 1
    while f"{prefix}_{index}" in existing:
        index += 1
    return f"{prefix}_{index}"


def build_cookie_record(name, cookies_str, login_api):
    cookies = trans_cookies(cookies_str)
    record = {
        "name": name,
        "cookies": cookies_str,
        "status": "active",
        "login_at": datetime.now().isoformat(timespec="seconds"),
        "daily_note_limit": DEFAULT_DAILY_NOTE_LIMIT_PER_ACCOUNT,
        "daily_comment_limit": DEFAULT_DAILY_COMMENT_LIMIT_PER_ACCOUNT,
        "max_concurrency": DEFAULT_ACCOUNT_CONCURRENCY_PER_ACCOUNT,
        "usage_date": datetime.now().date().isoformat(),
        "note_ids_today": [],
        "comments_today": 0,
    }
    if not cookies.get("web_session"):
        record["status"] = "expired"
        record["last_error"] = "登录结果缺少 web_session"
        return record

    try:
        success, user_info, _ = login_api.get_user_info(cookies)
    except Exception as e:
        record["status"] = "expired"
        record["last_error"] = str(e)
        return record

    if success:
        if user_info.get("nickname"):
            record["nickname"] = user_info["nickname"]
        if user_info.get("red_id"):
            record["red_id"] = user_info["red_id"]
    else:
        record["status"] = "expired"
        record["last_error"] = "获取用户信息失败"
    return record


def login_qr_batch(count, output_path=DEFAULT_OUTPUT, login_api=None, show_in_terminal=True, name_prefix="account"):
    login_api = login_api or XHSLoginApi()
    records = load_cookie_records(output_path)

    for _ in range(count):
        name = next_account_name(records, name_prefix)
        logger.info(f"开始扫码登录账号: {name}")
        cookies_str = login_api.qrcode_login(show_in_terminal=show_in_terminal)
        if not cookies_str:
            raise RuntimeError(f"{name} 扫码登录失败")

        records.append(build_cookie_record(name, cookies_str, login_api))
        save_cookie_records(output_path, records)
        logger.success(f"已保存账号 {name} 到 {output_path}")

    return records


def verify_crawl_cookie(cookies_str, crawl_api=None):
    owns_api = crawl_api is None
    crawl_api = crawl_api or XHS_Apis()
    try:
        success, message, _ = crawl_api.search_note("小红书", cookies_str, page=1)
        return success, message
    finally:
        if owns_api:
            crawl_api.close()


def import_cookie(cookies_str, output_path=DEFAULT_OUTPUT, login_api=None, crawl_api=None, name_prefix="account"):
    login_api = login_api or XHSLoginApi()
    records = load_cookie_records(output_path)
    name = next_account_name(records, name_prefix)
    record = build_cookie_record(name, cookies_str, login_api)
    if record["status"] != "active":
        raise RuntimeError(f"{name} Cookie 验证失败: {record.get('last_error', '未知错误')}")
    duplicate = next(
        (
            item for item in records
            if item.get("status") == "active" and item.get("red_id") == record.get("red_id")
        ),
        None,
    )
    if duplicate:
        raise RuntimeError(f"Cookie 已对应现有账号 {duplicate.get('name')}")
    success, message = verify_crawl_cookie(cookies_str, crawl_api)
    if not success:
        raise RuntimeError(f"{name} 采集接口验证失败: {message}")
    records.append(record)
    save_cookie_records(output_path, records)
    logger.success(f"已保存账号 {name} 到 {output_path}")
    return record


def build_parser():
    parser = argparse.ArgumentParser(description="小红书 PC 端 Cookie 批量管理")
    subparsers = parser.add_subparsers(dest="command", required=True)

    login_qr = subparsers.add_parser("login-qr", help="批量扫码登录并保存 Cookie")
    login_qr.add_argument("--count", type=int, default=1, help="需要扫码登录的账号数量")
    login_qr.add_argument("--output", default=DEFAULT_OUTPUT, help="Cookie JSON 输出文件")
    login_qr.add_argument("--name-prefix", default="account", help="账号名称前缀")
    login_qr.add_argument("--image", action="store_true", help="使用图片窗口展示二维码，默认在终端展示")

    import_cookie_parser = subparsers.add_parser("import-cookie", help="导入已登录浏览器的完整 Cookie")
    import_cookie_parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Cookie JSON 输出文件")
    import_cookie_parser.add_argument("--name-prefix", default="account", help="账号名称前缀")

    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.command == "login-qr":
        if args.count <= 0:
            raise ValueError("--count 必须大于 0")
        login_qr_batch(
            count=args.count,
            output_path=args.output,
            show_in_terminal=not args.image,
            name_prefix=args.name_prefix,
        )
    elif args.command == "import-cookie":
        print("请从已登录网页的 Network 请求头复制完整 Cookie 后粘贴到此处。")
        cookies_str = getpass.getpass("Cookie: ").strip()
        if not cookies_str:
            raise ValueError("Cookie 不能为空")
        import_cookie(
            cookies_str,
            output_path=args.output,
            name_prefix=args.name_prefix,
        )


if __name__ == "__main__":
    main()
