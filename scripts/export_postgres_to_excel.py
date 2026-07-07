import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from xhs_utils.common_util import init
from xhs_utils.pg_storage import PostgresStorage


def main(argv=None):
    parser = argparse.ArgumentParser(description="从 PostgreSQL 导出小红书采集数据到 Excel")
    parser.add_argument("--keyword", default="杭州电商")
    parser.add_argument("--output-dir", default="")
    args = parser.parse_args(argv)

    _, base_path = init()
    output_dir = args.output_dir or base_path["excel"]

    storage = PostgresStorage()
    try:
        storage.init_schema()
        note_file, comment_file = storage.export_to_excel(output_dir, args.keyword)
        print(f"笔记 Excel: {note_file}")
        print(f"评论 Excel: {comment_file}")
    finally:
        storage.close()


if __name__ == "__main__":
    main()
