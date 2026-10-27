"""Offline repository checks: no Azure credentials, Spark or DBUs required."""

import ast
import json
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
SDP = ROOT / "greenhouse_sdp" / "transformations"


class RepositoryChecks(unittest.TestCase):
    def test_job_yaml_files_are_parseable(self):
        for path in sorted((ROOT / "resources").glob("*.yml")):
            with self.subTest(path=path.name):
                data = yaml.safe_load(path.read_text(encoding="utf-8"))
                self.assertIsInstance(data, dict)
                self.assertIn("resources", data)

    def test_greenhouse_job_task_order(self):
        path = ROOT / "resources" / "greenhouse_ETL.yml"
        job = yaml.safe_load(path.read_text(encoding="utf-8"))[
            "resources"
        ]["jobs"]["greenhouse_etl_job"]
        tasks = {task["task_key"]: task for task in job["tasks"]}
        self.assertIn("greenhouse_api_extract", tasks)
        extract = tasks["greenhouse_api_extract"]
        self.assertIn("notebook_task", extract)
        pipeline_tasks = [t for t in tasks.values() if "pipeline_task" in t]
        self.assertEqual(len(pipeline_tasks), 1)
        self.assertEqual(
            pipeline_tasks[0].get("depends_on"),
            [{"task_key": "greenhouse_api_extract"}],
        )
        self.assertIn("boards", [p["name"] for p in job["parameters"]])

    def test_greenhouse_extractor_notebook_is_valid_python(self):
        notebook = ROOT / "bronze" / "04_greenhouse_api_extract.ipynb"
        data = json.loads(notebook.read_text(encoding="utf-8"))
        self.assertEqual(data["nbformat"], 4)
        code = "\n".join(
            "".join(cell["source"])
            if isinstance(cell["source"], list)
            else cell["source"]
            for cell in data["cells"]
            if cell["cell_type"] == "code"
        )
        self.assertTrue(code.strip())
        ast.parse(code, filename=str(notebook))
        self.assertIn('dbutils.widgets.get("boards")', code)
        self.assertIn("snapshot_id", code)

    def test_greenhouse_sql_sources_exist(self):
        files = ("bronze.sql", "silver.sql", "classification.sql",
                 "scd2.sql", "fanout.sql")
        for name in files:
            with self.subTest(file=name):
                path = SDP / name
                self.assertTrue(path.is_file(), f"Missing {path}")
                self.assertIn("CREATE", path.read_text(encoding="utf-8").upper())

    def test_sql_layer_contracts(self):
        bronze = (SDP / "bronze.sql").read_text(encoding="utf-8")
        silver = (SDP / "silver.sql").read_text(encoding="utf-8")
        scd2 = (SDP / "scd2.sql").read_text(encoding="utf-8")
        fanout = (SDP / "fanout.sql").read_text(encoding="utf-8")
        self.assertIn("greenhouse_snapshots/", bronze)
        self.assertIn("read_files(", bronze)
        self.assertIn("greenhouse_snapshots_clean", silver)
        self.assertIn("LATERAL EXPLODE", silver.upper())
        self.assertIn("KEYS (board_token, job_id)", scd2)
        self.assertIn("STORED AS SCD TYPE 2", scd2)
        self.assertEqual(fanout.upper().count("CREATE OR REFRESH MATERIALIZED VIEW"), 2)
        self.assertIn("greenhouse_quarantine", fanout)


if __name__ == "__main__":
    unittest.main()
