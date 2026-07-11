import json
import tempfile
import unittest
from pathlib import Path

from scripts.analyze_xhs_calibrations import analyze


class CalibrationAnalysisTests(unittest.TestCase):
    def test_smoke_run_does_not_become_capacity_recommendation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "smoke.calibration.json"
            path.write_text(json.dumps({
                "metadata": {
                    "account_names": ["account_1"],
                    "target_notes": 20,
                    "note_count": 20,
                    "target_comments": 200,
                    "comment_count": 200,
                    "active_account_capacity": 8,
                    "validation_after": {"success": True},
                },
                "http": {"error_rate": 0.0},
                "threshold_result": {"passed": True},
            }), encoding="utf-8")

            result = analyze([path])

        self.assertEqual(result["aggregate_safe_note_capacity"], 0)
        self.assertEqual(result["next_action"], "run_first_200_note_calibration")

    def test_200_note_pass_uses_eighty_percent_safe_capacity(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "full.calibration.json"
            path.write_text(json.dumps({
                "metadata": {
                    "account_names": ["account_1"],
                    "target_notes": 200,
                    "note_count": 200,
                    "target_comments": 2000,
                    "comment_count": 1800,
                    "active_account_capacity": 14,
                    "validation_after": {"success": True},
                },
                "http": {"error_rate": 0.0},
                "threshold_result": {"passed": True},
            }), encoding="utf-8")

            result = analyze([path])

        self.assertEqual(result["aggregate_safe_note_capacity"], 160)
        self.assertEqual(result["aggregate_safe_comment_capacity"], 1600)
        self.assertEqual(result["recommended_concurrency"], 14)


if __name__ == "__main__":
    unittest.main()
