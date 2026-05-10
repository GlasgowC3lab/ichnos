import json
import os
import tempfile
import unittest

from src.scripts.IchnosCF import normalize_node_governors
from src.utils.NodeConfigModelReader import get_model_governor
from src.utils.PowerModel import get_power_model_for_node


class NodeGovernorTests(unittest.TestCase):
    def test_invalid_json_governor_file_warns_and_is_ignored(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as handle:
            handle.write("{")
            path = handle.name

        try:
            with self.assertLogs("src.scripts.IchnosCF", level="WARNING") as logs:
                self.assertEqual(normalize_node_governors(path), {})
        finally:
            os.unlink(path)

        self.assertIn("Ignoring node-governors", "\n".join(logs.output))

    def test_unknown_node_governor_override_warns_and_falls_back(self):
        with self.assertLogs("src.utils.NodeConfigModelReader", level="WARNING") as logs:
            governor = get_model_governor(
                "gpgnode-04",
                "powersave_linear",
                {"gpgnode-04": "not-a-governor"},
            )

        self.assertEqual(governor, "powersave")
        self.assertIn("Ignoring node governor override", "\n".join(logs.output))

    def test_missing_model_type_for_selected_governor_has_clear_error(self):
        with self.assertRaisesRegex(ValueError, "does not define model type"):
            get_power_model_for_node(
                "gpgnode-22",
                "powersave_polynomial",
                {"gpgnode-22": "performance"},
            )

    def test_governor_file_supports_nested_governor_records(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as handle:
            json.dump({"gpgnode-04": {"dominant_governor": "performance"}}, handle)
            path = handle.name

        try:
            self.assertEqual(normalize_node_governors(path), {"gpgnode-04": "performance"})
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
