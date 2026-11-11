"""MOCKED unit tests for JSON-only quality CLI semantics."""

import json

from lakehouse.models.spec import SPECS
from lakehouse.quality import cli
from lakehouse.quality.contracts import ERROR, FAIL, PASS, QualityError, TableQualityReport


def _report(status, table="lakehouse.silver.customers"):
    return TableQualityReport(table, status, 1, 0, (),
                              QualityError("RuntimeError", "broken") if status == ERROR else None)


class _Spark:
    def __init__(self):
        self.stopped = False

    def stop(self):
        self.stopped = True


class _BrokenStopSpark(_Spark):
    def stop(self):
        super().stop()
        raise RuntimeError("Spark shutdown failed")


def test_cli_stdout_is_json_only_and_exit_codes(monkeypatch, capsys):
    spark = _Spark()
    monkeypatch.setattr(cli, "run_quality", lambda *_args, **_kwargs: (_report(PASS),))
    assert cli.main([], spark_factory=lambda: spark, configure=lambda _: None, specs={"customers": SPECS["customers"]}) == 0
    assert json.loads(capsys.readouterr().out)[0]["status"] == PASS
    assert spark.stopped

    monkeypatch.setattr(cli, "run_quality", lambda *_args, **_kwargs: (_report(FAIL),))
    assert cli.main([], spark_factory=_Spark, configure=lambda _: None, specs={"customers": SPECS["customers"]}) == 1

    monkeypatch.setattr(cli, "run_quality", lambda *_args, **_kwargs: (_report(ERROR),))
    assert cli.main([], spark_factory=_Spark, configure=lambda _: None, specs={"customers": SPECS["customers"]}) == 2


def test_runtime_table_error_emits_valid_json_and_exit_2(monkeypatch, capsys):
    monkeypatch.setattr(cli, "run_quality", lambda *_args, **_kwargs: (_report(ERROR),))

    assert cli.main(["--table-name", "customers"], spark_factory=_Spark, configure=lambda _: None,
                    specs={"customers": SPECS["customers"]}) == 2
    captured = capsys.readouterr()
    assert captured.err == ""
    assert json.loads(captured.out) == [_report(ERROR).as_dict()]


def test_cli_invalid_usage_and_pre_execution_configuration_are_exit_3(capsys):
    specs = {"customers": SPECS["customers"]}
    assert cli.main(["--table-name", "nope"], specs=specs) == 3
    assert cli.main(["--sample-limit", "-1"], specs=specs) == 3
    assert cli.main([], specs={}) == 3
    assert capsys.readouterr().out == ""


def test_configure_failure_is_structured_json_runtime_error(capsys):
    assert cli.main([], spark_factory=_Spark, configure=lambda _: (_ for _ in ()).throw(RuntimeError("no catalog")),
                    specs={"customers": SPECS["customers"]}) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["status"] == ERROR
    assert payload[0]["error"]["message"] == "no catalog"


def test_shutdown_failure_changes_otherwise_passing_output_to_error(monkeypatch, capsys):
    monkeypatch.setattr(cli, "run_quality", lambda *_args, **_kwargs: (_report(PASS),))

    assert cli.main([], spark_factory=_BrokenStopSpark, configure=lambda _: None,
                    specs={"customers": SPECS["customers"]}) == 2
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert len(payload) == 1
    assert payload[0]["status"] == ERROR
    assert payload[0]["error"] == {
        "error_class": "RuntimeError", "message": "Spark shutdown failed",
        "details": {"table": "lakehouse.silver.customers", "stage": "spark.stop"},
    }
    assert captured.err == "Spark shutdown failed\n"
