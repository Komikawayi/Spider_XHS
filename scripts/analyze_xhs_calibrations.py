# -*- coding: utf-8 -*-
"""汇总单账号异步标定报告，生成账号建议额度和三账号总容量。"""

import argparse
import json
from datetime import datetime
from pathlib import Path


def safe_limit(report, key):
    metadata = report["metadata"]
    if int(metadata.get("target_notes") or 0) < 200:
        return 0
    target_key = "target_notes" if key == "notes" else "target_comments"
    actual_key = "note_count" if key == "notes" else "comment_count"
    value = metadata.get(target_key) if report["threshold_result"]["passed"] else metadata.get(actual_key)
    return max(0, int((value or 0) * 0.8))


def analyze(paths):
    reports = [json.loads(Path(path).read_text(encoding="utf-8")) for path in paths]
    accounts = {}
    for report in reports:
        metadata = report["metadata"]
        names = metadata.get("account_names") or ["unknown"]
        account = names[0]
        entry = {
            "account": account,
            "concurrency": metadata.get("active_account_capacity"),
            "target_notes": metadata.get("target_notes"),
            "actual_notes": metadata.get("note_count"),
            "target_comments": metadata.get("target_comments"),
            "actual_comments": metadata.get("comment_count"),
            "passed": report["threshold_result"]["passed"],
            "capacity_qualified": bool(
                report["threshold_result"]["passed"]
                and int(metadata.get("target_notes") or 0) >= 200
            ),
            "http_error_rate": report["http"]["error_rate"],
            "validation_after": metadata.get("validation_after", {}).get("success"),
            "safe_note_limit": safe_limit(report, "notes"),
            "safe_comment_limit": safe_limit(report, "comments"),
            "recommended_cooldown_minutes": 30 if report["threshold_result"]["passed"] else 1440,
            "source": str(Path(metadata.get("metrics_file", "")) or ""),
        }
        previous = accounts.get(account)
        if previous is None or entry["target_notes"] >= previous["target_notes"]:
            accounts[account] = entry

    entries = sorted(accounts.values(), key=lambda item: item["account"])
    passed = [entry for entry in entries if entry["capacity_qualified"]]
    return {
        "generated_at": datetime.now().isoformat(),
        "account_count": len(entries),
        "accounts": entries,
        "recommended_concurrency": max(
            (entry["concurrency"] or 0 for entry in passed), default=0
        ),
        "aggregate_safe_note_capacity": sum(entry["safe_note_limit"] for entry in entries),
        "aggregate_safe_comment_capacity": sum(entry["safe_comment_limit"] for entry in entries),
        "next_action": (
            "run_first_200_note_calibration"
            if any(entry["target_notes"] < 200 for entry in entries)
            else (
                "increase_each_passed_account_by_50_notes_and_500_comments"
                if entries and all(entry["capacity_qualified"] for entry in entries)
                else "review_failed_account_metrics_and_swap_concurrency"
            )
        ),
    }


def build_parser():
    parser = argparse.ArgumentParser(description="汇总小红书账号容量标定结果")
    parser.add_argument("--log-dir", default="logs")
    parser.add_argument("--output", default="")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    paths = sorted(Path(args.log_dir).glob("*.calibration.json"))
    if not paths:
        raise SystemExit(f"没有找到标定报告: {args.log_dir}")
    result = analyze(paths)
    output = Path(args.output) if args.output else Path(args.log_dir) / "xhs_calibration_summary.json"
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
