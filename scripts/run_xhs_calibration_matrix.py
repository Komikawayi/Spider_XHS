# -*- coding: utf-8 -*-
"""顺序执行三个账号的200篇容量标定，并生成汇总报告。"""

import argparse
import subprocess
import sys
from pathlib import Path


def build_command(args, account, concurrency):
    script = Path(__file__).with_name("spider_hangzhou_ecommerce_async.py")
    return [
        sys.executable,
        str(script),
        "--query", args.query,
        "--require-num", str(args.require_num),
        "--target-comments", str(args.target_comments),
        "--max-comments-per-note", str(args.max_comments_per_note),
        "--note-concurrency", "20",
        "--comment-concurrency", "12",
        "--sign-concurrency", "12",
        "--account-name", account,
        "--calibration-mode",
        "--fixed-concurrency", str(concurrency),
        "--log-dir", args.log_dir,
    ]


def build_parser():
    parser = argparse.ArgumentParser(description="顺序执行三账号小红书容量标定")
    parser.add_argument("--accounts", nargs=3, required=True)
    parser.add_argument("--concurrencies", nargs=3, type=int, default=[8, 14, 20])
    parser.add_argument("--query", default="植村秀")
    parser.add_argument("--require-num", type=int, default=200)
    parser.add_argument("--target-comments", type=int, default=2000)
    parser.add_argument("--max-comments-per-note", type=int, default=100)
    parser.add_argument("--log-dir", default="logs")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if len(set(args.accounts)) != 3:
        raise SystemExit("--accounts 必须是三个不同账号")
    if any(value < 1 for value in args.concurrencies):
        raise SystemExit("并发必须大于等于1")

    for account, concurrency in zip(args.accounts, args.concurrencies):
        command = build_command(args, account, concurrency)
        print(f"开始标定 {account}: concurrency={concurrency}", flush=True)
        subprocess.run(command, check=True)

    analyzer = Path(__file__).with_name("analyze_xhs_calibrations.py")
    subprocess.run(
        [sys.executable, str(analyzer), "--log-dir", args.log_dir],
        check=True,
    )


if __name__ == "__main__":
    main()
