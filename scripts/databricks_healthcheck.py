"""Build local health reports from read-only Databricks CLI metadata."""

import argparse
import json
import re
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from tempfile import TemporaryDirectory
from uuid import uuid4


PROFILE = "DEFAULT"
RUN_LIMIT = 5
COMMAND_TIMEOUT = 30
MIN_RUNTIME_SAMPLES = 2
SLOW_RUNTIME_MULTIPLIER = 2
SLOW_RUNTIME_EXTRA_SECONDS = 60
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[1] / ".cache" / "databricks-health"
LANDING_ROOT = "dbfs:/Volumes/job_market/landing/raw_files"
FRESHNESS_LIMIT = (
    "Run success does not establish data freshness. Landing metadata covers only visible "
    "top-level source files. Bronze, Silver, and Gold data freshness was not verified."
)
ALERT_POLICY = (
    f"Repeated failure means failures in at least two of the {RUN_LIMIT} latest job runs. "
    f"A slow runtime is at least {SLOW_RUNTIME_MULTIPLIER} times the median and "
    f"at least {SLOW_RUNTIME_EXTRA_SECONDS} seconds longer, based on at least "
    f"{MIN_RUNTIME_SAMPLES} earlier successful samples for the same task or completed job path."
)


@dataclass(frozen=True)
class Job:
    job_id: int
    name: str
    task_keys: tuple[str, ...]
    landing_directory: str
    file_suffix: str


JOBS = (
    Job(
        294974101799492, "greenhouse_etl_job",
        ("greenhouse_api_extract", "greanhause_Pipeline"), "greenhouse_snapshots", ".json",
    ),
    Job(
        161503859714343, "Historical ETL",
        ("Bronze", "Silver", "Quality_Check", "Unification", "Gold"), "hacker_news", ".parquet",
    ),
    Job(
        936843786753298, "API ETL",
        ("api_extract", "check_new_posts", "api_bronze", "api_silver", "api_enrichment", "unification", "gold_analytics"),
        "hacker_news_api", ".jsonl",
    ),
)

READ_COMMANDS = {("jobs", "list-runs"), ("jobs", "get-run"), ("jobs", "get-run-output"), ("fs", "ls")}
ACTIVE_STATES = {
    "RUNNING": "running", "TERMINATING": "running",
    "PENDING": "pending", "QUEUED": "pending", "BLOCKED": "pending", "WAITING": "pending",
    "WAITING_FOR_RETRY": "pending",
}
TERMINAL_STATES = {
    "succeeded": {"SUCCESS"},
    "failed": {
        "FAILED", "TIMEDOUT", "EVICTED", "SUCCESS_WITH_FAILURES", "INTERNAL_ERROR",
        "DRIVER_ERROR", "CLUSTER_ERROR", "REPOSITORY_CHECKOUT_FAILED", "INVALID_CLUSTER_REQUEST",
        "WORKSPACE_RUN_LIMIT_EXCEEDED", "FEATURE_DISABLED", "CLUSTER_REQUEST_LIMIT_EXCEEDED",
        "STORAGE_ACCESS_ERROR", "RUN_EXECUTION_ERROR", "UNAUTHORIZED_ERROR",
        "LIBRARY_INSTALLATION_ERROR", "MAX_SPARK_CONTEXTS_EXCEEDED", "RESOURCE_NOT_FOUND",
        "INVALID_RUN_CONFIGURATION", "CLIENT_ERROR", "CLOUD_FAILURE", "MAX_JOB_QUEUE_SIZE_EXCEEDED",
        "BUDGET_POLICY_LIMIT_EXCEEDED", "BREAKING_CHANGE",
    },
    "skipped": {
        "SKIPPED", "EXCLUDED", "DISABLED", "UPSTREAM_FAILED", "UPSTREAM_CANCELED", "UPSTREAM_EVICTED",
        "MAXIMUM_CONCURRENT_RUNS_REACHED", "MAX_CONCURRENT_RUNS_EXCEEDED",
    },
    "canceled": {"CANCELED", "USER_CANCELED"},
}

# Construct messages from known signals. Arbitrary output can contain unlabeled credentials.
ERROR_RULES = (
    (r"invalid or duplicate board tokens", "The extractor rejected the board configuration.",
     "A board token may be missing, malformed, or repeated.",
     "Check the configured boards against the extractor's validation rules."),
    (r"invalid or incomplete api response|invalid job ids", "Greenhouse snapshot validation failed.",
     "A board may have returned an empty or incomplete snapshot or invalid job IDs.",
     "Compare the reported total, actual job count, and unique job IDs for the affected board."),
    (r"api lieferte kein item|ungültige zuordnung|weniger als zwei hiring-threads",
     "Hacker News response validation failed.", "A thread or comment may be missing or inconsistent.",
     "Check the item and parent IDs and compare the thread selection with a successful extraction."),
    (r"\b(?:unauthorized(?:_error)?|unauthenticated|permission_denied|permission denied|"
     r"insufficient_permissions|forbidden|access denied|cannot configure default credentials|invalid_grant)\b"
     r"|\b(?:401|403) client error\b",
     "Authentication or access was denied.", "The effective identity may lack access or need OAuth renewal.",
     "Check the read permissions or run identity for the affected resource and verify profile DEFAULT."),
    (r"\b(?:429 client error|too many requests|rate limit(?:ed| exceeded)?)\b",
     "The request was rate limited.", "A source or workspace request quota may have been reached.",
     "Compare request volume, retry timing, and the applicable quota."),
    (r"\b(?:table_or_view_not_found|path_not_found|resource_not_found|schema_not_found|"
     r"catalog_not_found|repository_checkout_failed|notebook_not_found|no such file)\b",
     "A required object, path, or revision could not be found.",
     "The deployed path or an upstream dependency may differ from the checkout.",
     "Compare the deployed notebook revision, the Job YAML task path, and the expected input location."),
    (r"\b(?:delta_failed_to_merge_fields|schema_mismatch|unresolved_column(?:\.\w+)?|"
     r"incompatible_data_for_table|analysisexception|schema mismatch)\b",
     "A schema or column could not be resolved.", "An input schema may have changed or drifted.",
     "Compare the Bronze schema and rescued fields with the Silver transformation and Gold contract."),
    (r"\bcheckpoint\b", "The error references streaming state or a checkpoint.",
     "The checkpoint may be inaccessible or incompatible with the current query.",
     "Compare the checkpoint path and stream changes with the last successful run before planning recovery."),
    (r"\b(?:timedout|readtimeout|connecttimeout|timeout(?:exception)?|timed out|deadline exceeded)\b",
     "An execution or request timed out.", "An upstream request, startup delay, or slow stage may have exceeded a limit.",
     "Compare startup and execution durations and locate the last completed stage in the task output."),
    (r"\b(?:cluster_error|driver_error|driver lost|outofmemoryerror|out of memory|cloud_failure|"
     r"invalid_cluster_request|cluster_request_limit_exceeded)\b",
     "Compute startup, driver, or cloud failure was reported.",
     "Startup capacity, compute configuration, or memory pressure may be involved.",
     "Inspect existing driver or cluster events and compare setup time with a successful run."),
    (r"\b(?:httperror|connectionerror|connection reset|name resolution|\d{3} server error)\b",
     "A network or HTTP request failed.", "The source API or network path may be unavailable.",
     "Check the reported HTTP status and endpoint availability in the task's existing error output."),
)


class CliError(Exception):
    """A safe description of a failed metadata read."""


def as_object(value):
    return value if isinstance(value, dict) else {}


def positive_int(value):
    return value if type(value) is int and 0 < value < 2**63 else None


def run_state(record):
    legacy = as_object(record.get("state"))
    current = as_object(record.get("status"))
    lifecycle = current.get("state") or legacy.get("life_cycle_state")
    if isinstance(lifecycle, str) and lifecycle in ACTIVE_STATES:
        return ACTIVE_STATES[lifecycle], lifecycle

    termination = as_object(current.get("termination_details"))
    for value in (legacy.get("result_state"), lifecycle, legacy.get("life_cycle_state"),
                  termination.get("code"), termination.get("type")):
        if isinstance(value, str):
            for status, reasons in TERMINAL_STATES.items():
                if value in reasons:
                    return status, value
    return "unknown", None


def state_signals(record):
    legacy = as_object(record.get("state"))
    termination = as_object(as_object(record.get("status")).get("termination_details"))
    return legacy.get("state_message"), termination.get("message"), termination.get("code")


def find_error_rule(signals, source):
    # Match only enough text to inspect the usual beginning and end of error traces.
    text = "\n".join(f"{value[:4096]}\n{value[-4096:]}" for value in signals if isinstance(value, str))
    for pattern, message, cause, step in ERROR_RULES:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return {"message": message, "possible_cause": cause, "next_step": step, "source": source}
    return None


def read_cli(executable, *arguments):
    if arguments[:2] not in READ_COMMANDS:
        raise ValueError("Only the configured metadata reads are supported.")

    command = [executable, *arguments, "--profile", PROFILE, "--output", "json"]
    try:
        response = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                                  encoding="utf-8", errors="replace", timeout=COMMAND_TIMEOUT, check=False)
    except subprocess.TimeoutExpired:
        raise CliError(f"Databricks CLI exceeded the {COMMAND_TIMEOUT}-second read timeout.") from None
    except OSError:
        raise CliError("Databricks CLI could not be started. Check PATH or --cli.") from None

    if response.returncode != 0:
        diagnosis = find_error_rule((response.stderr, response.stdout), "CLI error")
        detail = f" {diagnosis['message']}" if diagnosis else ""
        raise CliError(f"Databricks CLI read failed (exit {response.returncode}).{detail}")

    try:
        return json.loads(response.stdout)
    except (ValueError, RecursionError):
        raise CliError("Databricks CLI did not return valid JSON.") from None


def iso_utc(value):
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def from_epoch_ms(value):
    if positive_int(value) is None:
        return None
    try:
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=value)
    except OverflowError:
        return None


def execution_fields(record, now, *, job_run=False):
    status, reason = run_state(record)
    start = from_epoch_ms(record.get("start_time"))
    end = from_epoch_ms(record.get("end_time"))
    duration = None
    if status == "running" and start and now >= start:
        duration = (now - start).total_seconds()
    elif status in {"succeeded", "failed", "canceled"}:
        if start and end and end >= start:
            duration = (end - start).total_seconds()
        elif job_run and (run_duration := positive_int(record.get("run_duration"))):
            duration = run_duration / 1000

    return {"status": status, "reason": reason, "started_at_utc": iso_utc(start) if start else None,
            "duration_seconds": duration}


def collect_task(cli, task, job, aliases, now):
    key = task.get("task_key")
    label = None
    if isinstance(key, str) and key:
        label = key if key in job.task_keys else aliases.setdefault(key, f"Other task {len(aliases) + 1}")
    task_id = positive_int(task.get("run_id"))
    pipeline_id = as_object(task.get("pipeline_task")).get("pipeline_id")
    if not isinstance(pipeline_id, str) or not re.fullmatch(
        r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", pipeline_id
    ):
        pipeline_id = None

    report = {"task": label, "task_run_id": task_id, "pipeline_id": pipeline_id,
              **execution_fields(task, now)}
    if report["status"] != "failed":
        return report

    output = {}
    output_status = "unavailable"
    output_issue = "No task run ID was returned."
    if task_id:
        try:
            output = cli("jobs", "get-run-output", str(task_id))
            if not isinstance(output, dict):
                raise CliError("Task output had an unexpected JSON shape.")
            output_status = "truncated" if output.get("logs_truncated") is True else "read"
            output_issue = None
        except CliError as error:
            output_issue = str(error)
            output = {}

    diagnosis = (
        find_error_rule((output.get("error"), output.get("error_trace")), "task error output")
        or find_error_rule((*state_signals(as_object(output.get("metadata"))), *state_signals(task), report["reason"]),
                           "task run state")
        or find_error_rule((output.get("logs"),), "task logs")
    )
    report.update(output_status=output_status, output_issue=output_issue, diagnosis=diagnosis or {
        "message": "Task failed. The available fields contained no recognized error signal.",
        "possible_cause": "Available metadata is insufficient to identify a cause.",
        "next_step": "Inspect the existing output for the task run ID and compare it with a successful attempt.",
        "source": "task run state",
    })
    if pipeline_id:
        report["diagnosis"]["pipeline_next_step"] = (
            "Use `pipelines list-updates` to match the pipeline update to the task start time. "
            "Inspect the matching update with `pipelines list-pipeline-events` to locate the first failed dataset."
        )
    return report


def collect_run(cli, summary, job, aliases, now):
    run_id = summary["run_id"]
    detail = summary
    notes = []
    try:
        detail = cli("jobs", "get-run", str(run_id))
        if positive_int(as_object(detail).get("run_id")) != run_id:
            raise CliError("Run details did not identify the requested run.")
    except CliError as error:
        notes.append(str(error))
        detail = summary

    raw_tasks = detail.get("tasks", [])
    if raw_tasks is None:
        raw_tasks = []
    if not isinstance(raw_tasks, list) or any(not isinstance(task, dict) for task in raw_tasks):
        notes.append("The run returned malformed task metadata.")
        raw_tasks = []
    if detail.get("next_page_token") or detail.get("has_more") is True:
        notes.append("The run returned paginated metadata. The first page is shown.")

    tasks = [collect_task(cli, task, job, aliases, now) for task in raw_tasks]
    run = {"run_id": run_id, **execution_fields(detail, now, job_run=True),
           "tasks": tasks, "collection_notes": notes}
    if run["status"] == "failed" and all(task["status"] != "failed" for task in tasks):
        run["diagnosis"] = find_error_rule((*state_signals(detail), run["reason"]), "job run state") or {
            "message": "The job failed and the available metadata did not identify a failed task.",
            "possible_cause": "The job may have failed before a task started, or task metadata may be incomplete.",
            "next_step": "Inspect the job run's termination details and task graph for the first blocker.",
            "source": "job run state",
        }
    return run


def collect_job(cli, job, now):
    runs = []
    notes = []
    try:
        payload = cli("jobs", "list-runs", "--job-id", str(job.job_id), "--limit", str(RUN_LIMIT))
        summaries = payload.get("runs", []) if isinstance(payload, dict) else payload
        # The CLI can render a list directly or return null for an empty iterator.
        if summaries is None:
            summaries = []
        if not isinstance(summaries, list):
            raise CliError("The run list had an unexpected JSON shape.")
        if isinstance(payload, dict) and len(summaries) < RUN_LIMIT and (
            payload.get("next_page_token") or payload.get("has_more") is True
        ):
            notes.append("The CLI returned an incomplete page of run summaries.")

        aliases = {}
        seen = set()
        for summary in summaries[:RUN_LIMIT]:
            run_id = positive_int(as_object(summary).get("run_id"))
            if run_id is None or run_id in seen:
                notes.append("The run list contained an invalid or duplicate run ID.")
                continue
            seen.add(run_id)
            runs.append(collect_run(cli, summary, job, aliases, now))
    except CliError as error:
        notes.append(str(error))

    return {"job_id": job.job_id, "job": job.name, "runs": runs, "collection_notes": notes,
            "alerts": find_alerts(runs)}


def find_alerts(runs):
    failed_runs = [run["run_id"] for run in runs if run["status"] == "failed"]
    failed_tasks = defaultdict(list)
    runtime_groups = defaultdict(list)
    for run in runs:
        run_id = run["run_id"]
        tasks = run["tasks"]
        task_counts = Counter(task["task"] for task in tasks)
        for task in tasks:
            label = task["task"]
            if label and task["status"] == "failed" and run_id not in failed_tasks[label]:
                failed_tasks[label].append(run_id)
            if label and task_counts[label] == 1 and task["duration_seconds"]:
                runtime_groups[("task", label)].append((run_id, task))

        if (run["status"] == "succeeded" and run["duration_seconds"] and tasks
                and not run["collection_notes"]
                and all(task["task"] and task["status"] in {"succeeded", "skipped"} for task in tasks)):
            path = tuple(sorted((task["task"], task["status"]) for task in tasks))
            runtime_groups[("job", path)].append((run_id, run))

    alerts = []
    for label, run_ids in [(None, failed_runs), *failed_tasks.items()]:
        if len(run_ids) >= 2:
            alerts.append({"type": "repeated_failure", "task": label, "run_ids": run_ids})

    for (scope, _), samples in runtime_groups.items():
        # CLI run lists are newest first. Only older successes contribute to the baseline.
        for index, (run_id, execution) in enumerate(samples):
            baseline = [sample["duration_seconds"] for _, sample in samples[index + 1:]
                        if sample["status"] == "succeeded"]
            if len(baseline) < MIN_RUNTIME_SAMPLES:
                continue
            usual = median(baseline)
            seconds = execution["duration_seconds"]
            if seconds >= usual * SLOW_RUNTIME_MULTIPLIER and seconds - usual >= SLOW_RUNTIME_EXTRA_SECONDS:
                alerts.append({"type": "slow_runtime", "run_id": run_id, "task": execution.get("task"),
                               "task_run_id": execution.get("task_run_id"), "duration_seconds": seconds,
                               "baseline_seconds": usual, "baseline_samples": len(baseline), "scope": scope})
    return alerts


def landing_freshness(cli, job, now):
    path = f"{LANDING_ROOT}/{job.landing_directory}/"
    try:
        entries = cli("fs", "ls", path, "--long")
        if not isinstance(entries, list) or any(not isinstance(entry, dict) for entry in entries):
            raise CliError("The landing listing had an unexpected JSON shape.")
    except CliError as error:
        return {"status": "unavailable", "note": str(error)}

    modified_times = []
    snapshot_times = []
    for entry in entries:
        name = entry.get("name")
        if (entry.get("is_directory") is not False or not isinstance(name, str)
                or name.startswith((".", "_")) or not name.lower().endswith(job.file_suffix)):
            continue
        modified = entry.get("last_modified")
        if isinstance(modified, str):
            try:
                stamp = datetime.fromisoformat(modified.replace("Z", "+00:00"))
                if stamp.tzinfo and stamp.year > 1:
                    modified_times.append(stamp.astimezone(timezone.utc))
            except (ValueError, OverflowError):
                pass

        if job.landing_directory == "greenhouse_snapshots":
            match = re.fullmatch(r"greenhouse_([0-9]{8}T[0-9]{12}Z)_[a-z0-9_-]+\.json", name)
            if match:
                try:
                    snapshot_times.append(datetime.strptime(match[1], "%Y%m%dT%H%M%S%fZ").replace(tzinfo=timezone.utc))
                except ValueError:
                    pass

    latest_modified = max(modified_times, default=None)
    latest_snapshot = max(snapshot_times, default=None)
    observations = {}
    for key, stamp in (("file_modified", latest_modified), ("snapshot_collected", latest_snapshot)):
        if stamp:
            age_hours = (now - stamp).total_seconds() / 3600
            observations[key] = {"time_utc": iso_utc(stamp), "age_hours": round(age_hours, 2)}
    if not observations:
        return {"status": "no_evidence", "note": "No matching top-level files provided usable timestamps."}
    return {"status": "observed", **observations}


def build_report(cli, *, check_landing=False, now=None):
    now = now or datetime.now(timezone.utc)
    jobs = [collect_job(cli, job, now) for job in JOBS]
    for job, report in zip(JOBS, jobs):
        report["landing_freshness"] = (landing_freshness(cli, job, now) if check_landing
                                       else {"status": "not_requested"})
    complete = all(not job["collection_notes"] and all(not run["collection_notes"] for run in job["runs"])
                   for job in jobs)
    return {"checked_at_utc": iso_utc(now), "profile": PROFILE, "runs_per_job": RUN_LIMIT,
            "run_metadata_complete": complete, "alert_policy": ALERT_POLICY,
            "freshness_limit": FRESHNESS_LIMIT, "jobs": jobs}


def format_duration(seconds):
    return f"{seconds:.1f} s" if seconds is not None else "–"


def diagnosis_lines(diagnosis):
    lines = [f"- Observed error ({diagnosis['source']}) — {diagnosis['message']}",
             f"- Possible cause — {diagnosis['possible_cause']}",
             f"- Next step — {diagnosis['next_step']}"]
    if "pipeline_next_step" in diagnosis:
        lines.append(f"- Pipeline — {diagnosis['pipeline_next_step']}")
    return lines


def markdown(report):
    coverage = "complete" if report["run_metadata_complete"] else "partial"
    lines = ["# Databricks Pipeline Health", "",
             f"Collection started at {report['checked_at_utc']} with profile `{PROFILE}`.",
             f"Run metadata coverage is **{coverage}**.", "", report["freshness_limit"], "", report["alert_policy"]]
    for job in report["jobs"]:
        lines.extend(["", f"## {job['job']} ({job['job_id']})", ""])
        freshness = job["landing_freshness"]
        lines.append(f"Landing freshness evidence — {freshness['status'].replace('_', ' ')}.")
        if freshness.get("note"):
            lines.append(freshness["note"])
        for key, title in (("file_modified", "Latest file modification"), ("snapshot_collected", "Latest snapshot time")):
            if key in freshness:
                stamp = freshness[key]
                age = stamp["age_hours"]
                relative_time = f"{age:g} h old" if age >= 0 else f"{abs(age):g} h after collection started"
                lines.append(f"- {title} — {stamp['time_utc']} ({relative_time})")

        lines.extend(f"- Read issue — {note}" for note in job["collection_notes"])
        if not job["runs"]:
            lines.extend(["", "No runs available in the collected metadata."])
            continue

        lines.extend(["", "| Job run ID | Start (UTC) | Status | Duration |",
                      "| --- | --- | --- | ---: |"])
        for run in job["runs"]:
            lines.append(f"| {run['run_id']} | {run['started_at_utc'] or '–'} | {run['status']} "
                         f"| {format_duration(run['duration_seconds'])} |")

        if job["alerts"]:
            lines.extend(["", "### Alerts", ""])
            for alert in job["alerts"]:
                target = alert["task"] or "Job"
                if alert["type"] == "repeated_failure":
                    ids = ", ".join(map(str, alert["run_ids"]))
                    lines.append(f"- {target} failed in runs {ids}.")
                else:
                    lines.append(f"- {target} ran for {format_duration(alert['duration_seconds'])} in run "
                                 f"{alert['run_id']}. The earlier median was {format_duration(alert['baseline_seconds'])} "
                                 f"from {alert['baseline_samples']} samples.")

        for run in job["runs"]:
            lines.extend(["", f"### Run {run['run_id']}", ""])
            if run["reason"]:
                lines.extend([f"Run state detail — `{run['reason']}`.", ""])
            if run["collection_notes"]:
                lines.extend([*(f"- Read issue — {note}" for note in run["collection_notes"]), ""])
            if "diagnosis" in run:
                lines.extend([*diagnosis_lines(run["diagnosis"]), ""])
            if not run["tasks"]:
                lines.append("The CLI returned no task details for this run.")
                continue
            lines.extend(["| Task | Task run ID | Status | State detail | Duration |",
                          "| --- | --- | --- | --- | ---: |"])
            for task in run["tasks"]:
                lines.append(f"| {task['task'] or 'Unidentified task'} | {task['task_run_id'] or '–'} | "
                             f"{task['status']} | {task['reason'] or '–'} | {format_duration(task['duration_seconds'])} |")
            for task in run["tasks"]:
                if "diagnosis" not in task:
                    continue
                lines.extend(["", f"**{task['task'] or 'Unidentified task'} · task run {task['task_run_id'] or 'unknown'}**", ""])
                if task["pipeline_id"]:
                    lines.append(f"- Pipeline ID — `{task['pipeline_id']}`")
                if task["output_issue"]:
                    lines.append(f"- Task output unavailable — {task['output_issue']}")
                if task["output_status"] == "truncated":
                    lines.append("- The CLI reported truncated task logs.")
                lines.extend(diagnosis_lines(task["diagnosis"]))
    return "\n".join(lines) + "\n"


def write_reports(report, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = output_dir / f"{timestamp}-{uuid4().hex[:8]}"
    # Publish both formats together after they have been written to the same filesystem.
    with TemporaryDirectory(dir=output_dir, prefix=".draft-") as directory:
        draft = Path(directory)
        (draft / "health-report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        (draft / "health-report.md").write_text(markdown(report), encoding="utf-8")
        draft.rename(target)
    return target


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, epilog=(
        "Exit 0 means complete run metadata. Exit 1 means partial run metadata. "
        "Exit 2 means invalid arguments, missing CLI, or a local write error. "
        "Task-output and landing read gaps are shown in the reports."
    ))
    parser.add_argument("--cli", help="Path to the Databricks CLI executable. Defaults to databricks on PATH.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                        help="Parent directory for reports. Defaults to .cache/databricks-health in this repository.")
    parser.add_argument("--landing-freshness", action="store_true",
                        help="Also read Unity Catalog landing-directory metadata without starting compute.")
    args = parser.parse_args(argv)
    executable = shutil.which(args.cli or "databricks")
    if executable is None:
        parser.error("Databricks CLI executable not found. Add it to PATH or pass --cli.")

    def cli(*arguments):
        return read_cli(executable, *arguments)

    report = build_report(cli, check_landing=args.landing_freshness)
    try:
        target = write_reports(report, args.output_dir)
    except OSError:
        print("Could not write the local reports. Check --output-dir and local permissions.", file=sys.stderr)
        return 2
    print(f"Health reports written to {target}")
    return 0 if report["run_metadata_complete"] else 1


if __name__ == "__main__":
    sys.exit(main())
