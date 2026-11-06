"""`summarize --check`: the README's generated blocks and its hand-written test and mutant counts must match the
committed results files, and those must match the mutation harness. Each test breaks one thing in memory."""
import json
import shutil

import pytest

from lakehouse import summarize
from lakehouse.config import REPO

RES = REPO / "results"
README = (REPO / "README.md").read_text(encoding="utf-8")


def test_committed_readme_passes_the_check():
    assert summarize.problems(README, RES) == []


def test_mutation_files_match_the_harness():
    h = summarize.harness_counts()
    mu = json.loads((RES / "mutation.json").read_text(encoding="utf-8"))
    ms = json.loads((RES / "mutation_spark.json").read_text(encoding="utf-8"))
    assert (mu["total"], mu["python"], mu["sql"], mu["reviewer"]) == (h["total"], h["python"], h["sql"], h["reviewer"])
    assert ms["total"] == h["spark_total"] == h["spark_batch"] + h["spark_stream"]


@pytest.mark.parametrize("old,new", [
    ("planted bugs in the Spark code killed** (", "planted bugs in the Spark code killed** (1"),  # 13 -> 113
    ("`tests/test_spark.py`, ", "`tests/test_spark.py`, 1"),  # 9 -> 19 tests
])
def test_a_wrong_hand_written_spark_count_fails(old, new):
    assert README.count(old) == 1
    bad = summarize.problems(README.replace(old, new), RES)
    assert bad and all("block differs" not in b for b in bad)


def test_a_stale_pytest_count_anywhere_fails():
    mu = json.loads((RES / "mutation.json").read_text(encoding="utf-8"))
    text = README + f"\nAn old note: {mu['tests_passed'] - 24} pytest tests pass.\n"
    assert any("pytest tests" in b for b in summarize.problems(text, RES))


def test_a_stale_mutant_total_anywhere_fails():
    text = README + "\nThe complete harness now has 71 mutants.\n"
    assert any("71 mutants" in b for b in summarize.problems(text, RES))


def test_dropping_a_checked_sentence_fails():
    assert README.count(" in the batch models, ") == 1
    text = README.replace(" in the batch models, ", " in batch, ")
    assert any("no longer states" in b for b in summarize.problems(text, RES))


@pytest.mark.parametrize("block", list(summarize.BLOCKS))
def test_an_edited_generated_block_fails(block):
    start, end = summarize._markers(block)
    body = README.split(start, 1)[1].split(end, 1)[0]
    i = next(i for i, c in enumerate(body) if c.isdigit())
    edited = body[:i] + str((int(body[i]) + 1) % 10) + body[i + 1:]
    text = README.replace(start + body + end, start + edited + end)
    assert f"README.md {block} block differs from the results files; run `make summary`" in \
        summarize.problems(text, RES)


def test_a_results_file_that_disagrees_with_the_harness_fails(tmp_path):
    res = tmp_path / "results"
    shutil.copytree(RES, res)
    mu = json.loads((res / "mutation.json").read_text(encoding="utf-8"))
    mu["total"] = mu["killed"] = mu["total"] - 1
    (res / "mutation.json").write_text(json.dumps(mu), encoding="utf-8")
    assert any("tests/mutate.py defines" in b for b in summarize.problems(README, res))


def test_escaped_mutants_fail(tmp_path):
    res = tmp_path / "results"
    shutil.copytree(RES, res)
    ms = json.loads((res / "mutation_spark.json").read_text(encoding="utf-8"))
    ms["killed"] -= 1
    ms["escaped"] = ["spark_jobs.py: something"]
    (res / "mutation_spark.json").write_text(json.dumps(ms), encoding="utf-8")
    assert any("escaped" in b for b in summarize.problems(README, res))


def test_a_spark_results_file_that_disagrees_with_the_harness_fails(tmp_path):
    res = tmp_path / "results"
    shutil.copytree(RES, res)
    ms = json.loads((res / "mutation_spark.json").read_text(encoding="utf-8"))
    ms["total"] = ms["killed"] = ms["total"] + 1
    (res / "mutation_spark.json").write_text(json.dumps(ms), encoding="utf-8")
    assert any("Spark mutants; rerun" in b for b in summarize.problems(README, res))


def test_fewer_killed_than_planted_fails_even_without_an_escaped_list(tmp_path):
    res = tmp_path / "results"
    shutil.copytree(RES, res)
    mu = json.loads((res / "mutation.json").read_text(encoding="utf-8"))
    mu["killed"] -= 1
    (res / "mutation.json").write_text(json.dumps(mu), encoding="utf-8")
    assert any("escaped" in b for b in summarize.problems(README, res))
