import tempfile
import unittest
from pathlib import Path

import openpyxl

from xhs_utils.pg_storage import DEFAULT_DSN, PostgresStorage, SCHEMA_SQL, export_rows_to_excel, normalize_note_row, normalize_comment_row


class PgStorageHelperTests(unittest.TestCase):
    def test_normalize_note_row_preserves_raw_json_and_lists(self):
        note = {
            "note_id": "n1",
            "note_url": "https://example.com/n1",
            "note_type": "图集",
            "user_id": "u1",
            "nickname": "nick",
            "title": "title",
            "desc": "desc",
            "liked_count": "12",
            "collected_count": "3",
            "comment_count": "4",
            "share_count": "5",
            "image_list": ["img1", "img2"],
            "tags": ["杭州", "电商"],
        }

        row = normalize_note_row(note, keyword="杭州电商", raw={"raw": True})

        self.assertEqual(row["note_id"], "n1")
        self.assertEqual(row["keyword"], "杭州电商")
        self.assertEqual(row["liked_count"], 12)
        self.assertEqual(row["image_list"], ["img1", "img2"])
        self.assertEqual(row["raw_json"], {"raw": True})

    def test_normalize_comment_row_sets_parent_for_child_comment(self):
        comment = {
            "comment_id": "child1",
            "note_id": "n1",
            "content": "hello",
            "user_id": "u1",
            "user_nickname": "nick",
            "like_count": "8",
        }

        row = normalize_comment_row(comment, note_id="n1", note_title="title", parent_comment_id="root1", raw={"raw": True})

        self.assertEqual(row["parent_comment_id"], "root1")
        self.assertEqual(row["like_count"], 8)
        self.assertEqual(row["raw_json"], {"raw": True})

    def test_export_rows_to_excel_writes_headers_and_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "rows.xlsx"
            export_rows_to_excel([{"a": 1, "b": ["x", "y"]}], out, ["a", "b"])

            wb = openpyxl.load_workbook(out, read_only=True)
            ws = wb.active
            values = list(ws.iter_rows(values_only=True))
            wb.close()

        self.assertEqual(values[0], ("a", "b"))
        self.assertEqual(values[1], (1, '["x", "y"]'))

    def test_resolve_dsn_uses_default_when_env_missing(self):
        self.assertEqual(PostgresStorage.resolve_dsn(None, {}), DEFAULT_DSN)

    def test_resolve_dsn_prefers_explicit_dsn(self):
        self.assertEqual(PostgresStorage.resolve_dsn("postgresql://custom", {}), "postgresql://custom")

    def test_schema_adds_comment_progress_columns_without_dropping_tables(self):
        self.assertIn("ALTER TABLE xhs_notes ADD COLUMN IF NOT EXISTS comments_crawl_status", SCHEMA_SQL)
        self.assertIn("ALTER TABLE xhs_notes ADD COLUMN IF NOT EXISTS comments_next_cursor", SCHEMA_SQL)
        self.assertIn("ALTER TABLE xhs_notes ADD COLUMN IF NOT EXISTS comments_has_more", SCHEMA_SQL)

    def test_normalize_note_row_defaults_comment_progress(self):
        row = normalize_note_row({"note_id": "n1"}, keyword="植村秀")

        self.assertEqual(row["comments_crawl_status"], "pending")
        self.assertEqual(row["comments_collected_count"], 0)
        self.assertEqual(row["comments_target_count"], 0)
        self.assertEqual(row["comments_next_cursor"], "")
        self.assertTrue(row["comments_has_more"])


if __name__ == "__main__":
    unittest.main()
