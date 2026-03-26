from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from paper_analysis_dataset.services.annotator_selection import (
    build_annotator,
    default_annotator_selection_path,
    read_annotator_selection,
    resolve_annotation_backend,
    write_annotator_selection,
)


class AnnotatorSelectionTests(unittest.TestCase):
    def test_default_path_reads_from_user_private_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(os.environ, {"PAPER_ANALYSIS_DATASET_HOME": temp_dir}, clear=False):
                path = default_annotator_selection_path()

        self.assertEqual(Path(temp_dir) / "annotation_backend.json", path)

    def test_default_backend_is_codex_cli(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {
                    "PAPER_ANALYSIS_DATASET_HOME": temp_dir,
                    "PAPER_ANALYSIS_ANNOTATOR_BACKEND": "",
                },
                clear=False,
            ):
                backend = resolve_annotation_backend()

        self.assertEqual("codex_cli", backend)

    def test_env_override_beats_selection_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            selection_path = Path(temp_dir) / "annotation_backend.json"
            write_annotator_selection(
                "doubao",
                selection_config_path=selection_path,
                source="test",
            )
            with patch.dict(os.environ, {"PAPER_ANALYSIS_ANNOTATOR_BACKEND": "codex_cli"}, clear=False):
                backend = resolve_annotation_backend(selection_config_path=selection_path)

        self.assertEqual("codex_cli", backend)

    def test_write_and_read_selection_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            selection_path = Path(temp_dir) / "annotation_backend.json"
            write_annotator_selection(
                "codex_cli",
                selection_config_path=selection_path,
                source="test",
            )
            selection = read_annotator_selection(selection_path)

        self.assertIsNotNone(selection)
        assert selection is not None
        self.assertEqual("codex_cli", selection.selected_backend)

    def test_build_annotator_returns_named_backends(self) -> None:
        doubao = build_annotator("doubao", doubao_runner=lambda _: {"success": True, "content": "{}"})
        codex = build_annotator("codex_cli", codex_runner=lambda _: "{}")

        self.assertEqual("doubao", doubao.labeler_id)
        self.assertEqual("codex_cli", codex.labeler_id)


if __name__ == "__main__":
    unittest.main()
