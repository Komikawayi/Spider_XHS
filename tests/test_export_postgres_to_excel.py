
import unittest
from unittest.mock import patch

from scripts import export_postgres_to_excel as exporter


class FakeStorage:
    calls = []

    def __init__(self):
        FakeStorage.calls = []

    def init_schema(self):
        FakeStorage.calls.append("init_schema")

    def export_to_excel(self, output_dir, keyword=None):
        FakeStorage.calls.append("export_to_excel")
        return "notes.xlsx", "comments.xlsx"

    def close(self):
        FakeStorage.calls.append("close")


class ExportPostgresToExcelTests(unittest.TestCase):
    @patch("scripts.export_postgres_to_excel.init", return_value=("cookie", {"excel": "out"}))
    @patch("scripts.export_postgres_to_excel.PostgresStorage", FakeStorage)
    def test_main_initializes_schema_before_export(self, _init):
        exporter.main(["--keyword", "植村秀"])

        self.assertEqual(FakeStorage.calls, ["init_schema", "export_to_excel", "close"])


if __name__ == "__main__":
    unittest.main()
