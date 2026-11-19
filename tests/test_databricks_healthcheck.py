import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from scripts import databricks_healthcheck as healthcheck


NOW = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)
CLI = "stub-databricks"
SECRET = "private-sample-value-4fccdb9168"


def list_command(job):
    return ("jobs", "list-runs", "--job-id", str(job.job_id), "--limit", "5")


def execution(run_id, age_minutes, duration, result="SUCCESS", **fields):
    start = NOW - timedelta(minutes=age_minutes)
    end = start + timedelta(seconds=duration)
    return {
        "run_id": run_id,
        "start_time": int(start.timestamp() * 1000),
        "end_time": int(end.timestamp() * 1000),
        "state": {"life_cycle_state": "TERMINATED", "result_state": result},
        **fields,
    }


class HealthcheckTests(unittest.TestCase):
    def cli_stub(self, responses):
        def run(command, **kwargs):
            self.assertEqual(command[0], CLI)
            self.assertEqual(command[-4:], ["--profile", "DEFAULT", "--output", "json"])
            self.assertTrue(kwargs["capture_output"])
            self.assertEqual(kwargs["stdin"], subprocess.DEVNULL)
            payload = responses[tuple(command[1:-4])]
            if isinstance(payload, subprocess.CompletedProcess):
                return payload
            return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")

        return run

    def read(self, *arguments):
        return healthcheck.read_cli(CLI, *arguments)

    def test_statuses_distinguish_active_skipped_and_direct_failures(self):
        cases = (
            ({"state": {"result_state": "SUCCESS"}}, ("succeeded", "SUCCESS")),
            ({"state": {"life_cycle_state": "RUNNING"}}, ("running", "RUNNING")),
            ({"status": {"state": "QUEUED"}}, ("pending", "QUEUED")),
            ({"state": {"life_cycle_state": "SKIPPED"}}, ("skipped", "SKIPPED")),
            ({"state": {"result_state": "UPSTREAM_FAILED"}}, ("skipped", "UPSTREAM_FAILED")),
            ({"state": {"result_state": "CANCELED"}}, ("canceled", "CANCELED")),
            ({"state": {"result_state": "TIMEDOUT"}}, ("failed", "TIMEDOUT")),
            ({"state": {"result_state": SECRET}}, ("unknown", None)),
            ({"status": {"state": "TERMINATED", "termination_details": {"code": "CLUSTER_ERROR"}}},
             ("failed", "CLUSTER_ERROR")),
            ({"status": {"state": "TERMINATED", "termination_details": {"code": "SKIPPED"}}},
             ("skipped", "SKIPPED")),
        )
        for metadata, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(healthcheck.run_state(metadata), expected)

    def test_collects_five_runs_detects_alerts_and_never_serializes_raw_output(self):
        greenhouse, historical, api = healthcheck.JOBS
        responses = {
            list_command(greenhouse): [{"run_id": value} for value in range(105, 99, -1)],
            list_command(historical): {"runs": []},
            list_command(api): [{"run_id": 205}, {"run_id": 204}],
        }
        pipeline = {"pipeline_id": "12345678-1234-1234-1234-123456789abc"}
        for index, duration in enumerate((40, 35, 260, 50, 60)):
            run_id = 105 - index
            task_id = 1000 + run_id
            age = (index + 1) * 10
            result = "FAILED" if index < 2 else "SUCCESS"
            task = execution(task_id, age, duration - 5, result, task_key="greanhause_Pipeline",
                             pipeline_task=pipeline)
            responses["jobs", "get-run", str(run_id)] = execution(run_id, age, duration, result, tasks=[task])
            if result == "FAILED":
                responses["jobs", "get-run-output", str(task_id)] = {
                    "error": f"[DELTA_FAILED_TO_MERGE_FIELDS] schema mismatch, auth value {SECRET}",
                    "error_trace": f"trace {SECRET}",
                    "logs": f"raw log {SECRET}",
                    "logs_truncated": True,
                    "notebook_output": {"result": SECRET},
                    "metadata": {"resolved_values": {"parameters": {"boards": SECRET}}},
                }
        responses["jobs", "get-run", "205"] = execution(205, 2, 0, state={"life_cycle_state": "RUNNING"}, tasks=[
            execution(2001, 1, 0, task_key="api_extract", status={"state": "RUNNING"}),
            {"run_id": 2002, "task_key": SECRET, "status": {"state": "PENDING"}},
        ])
        responses["jobs", "get-run", "204"] = execution(204, 8, 40, tasks=[
            execution(2003, 8, 30, task_key="api_extract"),
            {"run_id": 2004, "task_key": "api_bronze", "state": {"result_state": "EXCLUDED"}},
        ])

        with patch.object(healthcheck.subprocess, "run", side_effect=self.cli_stub(responses)) as process:
            report = healthcheck.build_report(self.read, now=NOW)

        self.assertEqual(process.call_count, len(responses))
        self.assertTrue(report["run_metadata_complete"])
        greenhouse_report, historical_report, api_report = report["jobs"]
        self.assertEqual([run["run_id"] for run in greenhouse_report["runs"]], [105, 104, 103, 102, 101])
        self.assertEqual(historical_report["runs"], [])
        self.assertEqual({a["scope"] for a in greenhouse_report["alerts"] if a["type"] == "slow_runtime"},
                         {"task", "job"})
        self.assertEqual([a["run_ids"] for a in greenhouse_report["alerts"] if a["type"] == "repeated_failure"],
                         [[105, 104], [105, 104]])
        self.assertEqual([task["status"] for task in api_report["runs"][0]["tasks"]], ["running", "pending"])
        self.assertEqual(api_report["runs"][0]["tasks"][0]["duration_seconds"], 60)
        self.assertEqual(api_report["runs"][1]["tasks"][1]["status"], "skipped")
        self.assertEqual(api_report["landing_freshness"]["status"], "not_requested")
        failed = greenhouse_report["runs"][0]["tasks"][0]
        self.assertEqual(failed["task_run_id"], 1105)
        self.assertEqual(failed["output_status"], "truncated")
        self.assertIn("schema", failed["diagnosis"]["message"])
        self.assertEqual(failed["diagnosis"]["source"], "task error output")
        self.assertIn("pipeline_next_step", failed["diagnosis"])
        self.assertNotIn(SECRET, json.dumps(report))
        rendered = healthcheck.markdown(report)
        self.assertIn("task run 1105", rendered)
        self.assertIn("truncated task logs", rendered)
        self.assertIn("Run success does not establish data freshness", rendered)
        self.assertNotIn(SECRET, rendered)

    def test_unavailable_reads_keep_partial_reports_and_fall_back_to_state_message(self):
        greenhouse, historical, api = healthcheck.JOBS
        denied = subprocess.CompletedProcess([], 1, f"Bearer {SECRET}", f"PERMISSION_DENIED {SECRET}")
        historical_summary = execution(305, 10, 12, "FAILED")
        responses = {
            list_command(greenhouse): denied,
            list_command(historical): {"runs": [historical_summary]},
            ("jobs", "get-run", "305"): denied,
            list_command(api): [{"run_id": 205}],
            ("jobs", "get-run", "205"): execution(205, 10, 49, "FAILED", tasks=[
                execution(206, 10, 45, task_key="api_extract",
                          state={"result_state": "FAILED", "state_message": f"429 Client Error {SECRET}"}),
                {"task_key": "check_new_posts", "state": {"result_state": "UPSTREAM_FAILED"}},
            ]),
            ("jobs", "get-run-output", "206"): denied,
        }
        with patch.object(healthcheck.subprocess, "run", side_effect=self.cli_stub(responses)):
            report = healthcheck.build_report(self.read, now=NOW)

        self.assertFalse(report["run_metadata_complete"])
        greenhouse_report, historical_report, api_report = report["jobs"]
        self.assertIn("access was denied", greenhouse_report["collection_notes"][0])
        self.assertEqual(historical_report["runs"][0]["status"], "failed")
        self.assertIn("did not identify a failed task", historical_report["runs"][0]["diagnosis"]["message"])
        failed, skipped = api_report["runs"][0]["tasks"]
        self.assertEqual(failed["output_status"], "unavailable")
        self.assertIn("rate limited", failed["diagnosis"]["message"])
        self.assertEqual(failed["diagnosis"]["source"], "task run state")
        self.assertEqual(skipped["status"], "skipped")
        self.assertNotIn(SECRET, json.dumps(report))

    def test_success_with_different_tasks_skipped_is_not_a_comparable_job_runtime(self):
        entries = [
            execution(205, 10, 360, tasks=[
                execution(225, 10, 310, task_key="api_bronze"),
            ]),
            execution(204, 20, 30, tasks=[
                {"task_key": "api_bronze", "state": {"result_state": "EXCLUDED"}},
            ]),
            execution(203, 30, 20, tasks=[
                {"task_key": "api_bronze", "state": {"result_state": "EXCLUDED"}},
            ]),
        ]
        api = healthcheck.JOBS[2]
        responses = {list_command(api): entries}
        responses.update({("jobs", "get-run", str(entry["run_id"])): entry for entry in entries})
        with patch.object(healthcheck.subprocess, "run", side_effect=self.cli_stub(responses)):
            report = healthcheck.collect_job(self.read, api, NOW)
        self.assertEqual(report["alerts"], [])

    def test_landing_freshness_uses_file_metadata_and_greenhouse_snapshot_name(self):
        greenhouse, historical, api = healthcheck.JOBS
        responses = {list_command(job): None for job in healthcheck.JOBS}
        listings = (
            [{"name": f"greenhouse_20261118T110000123456Z_{SECRET}.json", "is_directory": False,
              "last_modified": "2026-11-18T13:00:00+01:00"},
             {"name": "directory.json", "is_directory": True, "last_modified": "2026-11-19T11:00:00Z"}],
            [],
            [{"name": f"{SECRET}.jsonl", "is_directory": False, "last_modified": "2026-11-18T06:00:00Z"}],
        )
        for job, listing in zip((greenhouse, historical, api), listings):
            responses["fs", "ls", f"{healthcheck.LANDING_ROOT}/{job.landing_directory}/", "--long"] = listing

        with patch.object(healthcheck.subprocess, "run", side_effect=self.cli_stub(responses)):
            report = healthcheck.build_report(self.read, check_landing=True, now=NOW)

        greenhouse_freshness, historical_freshness, api_freshness = [job["landing_freshness"] for job in report["jobs"]]
        self.assertEqual(greenhouse_freshness["file_modified"]["time_utc"], "2026-11-18T12:00:00.000Z")
        self.assertEqual(greenhouse_freshness["snapshot_collected"]["age_hours"], 25)
        self.assertEqual(historical_freshness["status"], "no_evidence")
        self.assertEqual(api_freshness["file_modified"]["age_hours"], 30)
        self.assertNotIn("snapshot_collected", api_freshness)
        self.assertNotIn(SECRET, json.dumps(report))

    def test_read_cli_refuses_other_commands_without_spawning_a_process(self):
        with patch.object(healthcheck.subprocess, "run") as process:
            with self.assertRaises(ValueError):
                self.read("jobs", "run-now", "123")
        process.assert_not_called()

    def test_invalid_json_and_timeouts_discard_captured_output(self):
        arguments = ("jobs", "get-run", "123")
        invalid = subprocess.CompletedProcess([], 0, SECRET, SECRET)
        with patch.object(healthcheck.subprocess, "run", return_value=invalid):
            with self.assertRaisesRegex(healthcheck.CliError, "valid JSON") as error:
                self.read(*arguments)
            self.assertNotIn(SECRET, str(error.exception))

        timeout = subprocess.TimeoutExpired(arguments, 30, output=SECRET, stderr=SECRET)
        with patch.object(healthcheck.subprocess, "run", side_effect=timeout):
            with self.assertRaisesRegex(healthcheck.CliError, "timeout") as error:
                self.read(*arguments)
            self.assertNotIn(SECRET, str(error.exception))

    def test_main_writes_paired_reports_and_returns_status_for_read_gaps(self):
        responses = {list_command(job): [] for job in healthcheck.JOBS}
        responses[list_command(healthcheck.JOBS[0])] = subprocess.CompletedProcess([], 1, "", SECRET)
        with TemporaryDirectory() as directory:
            with (patch.object(healthcheck.shutil, "which", return_value=CLI),
                  patch.object(healthcheck.subprocess, "run", side_effect=self.cli_stub(responses)),
                  patch("sys.stdout", new_callable=io.StringIO)):
                result = healthcheck.main(["--output-dir", directory])
            self.assertEqual(result, 1)
            report_dir, = Path(directory).iterdir()
            self.assertEqual({path.name for path in report_dir.iterdir()}, {"health-report.md", "health-report.json"})
            self.assertEqual(len(json.loads((report_dir / "health-report.json").read_text(encoding="utf-8"))["jobs"]), 3)
            rendered = (report_dir / "health-report.md").read_text(encoding="utf-8")
            self.assertIn("Run metadata coverage is **partial**", rendered)
            self.assertNotIn(SECRET, rendered)


if __name__ == "__main__":
    unittest.main()
