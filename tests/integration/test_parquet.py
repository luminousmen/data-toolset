
import subprocess
from pathlib import Path

import pytest
from utils import TEST_DATA_DIR
import pyarrow.parquet as pq


# @TODO: consolidate file paths somewhere
@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata2.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata3.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata4.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata5.parquet",
    ],
)
def test_head_command(file_path):
    result = subprocess.run(["data-toolset", "head", file_path, "-n", "10"], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ''
    assert len(result.stdout) > 0


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata2.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata3.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata4.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata5.parquet",
    ],
)
def test_tail_command(file_path):
    result = subprocess.run(["data-toolset", "tail", file_path, "-n", "10"], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ''
    assert len(result.stdout) > 0


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata2.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata3.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata4.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata5.parquet",
    ],
)
def test_meta_command(file_path):
    result = subprocess.run(["data-toolset", "meta", file_path], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ''
    # @TODO: check the result
    assert len(result.stdout) > 0


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata2.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata3.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata4.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata5.parquet",
    ],
)
def test_schema_command(file_path):
    result = subprocess.run(["data-toolset", "schema", file_path], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ''
    # @TODO: check the result
    assert len(result.stdout) > 0


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata2.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata3.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata4.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata5.parquet",
    ],
)
def test_stats_command(file_path):
    result = subprocess.run(["data-toolset", "stats", file_path], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ''
    # @TODO: check the result
    assert len(result.stdout) > 0


def test_validate_command():
    pass


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata2.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata3.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata4.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata5.parquet",
    ],
)
def test_query_command(file_path):
    query = f"select first_name from '{file_path.name}' where gender='Female'"

    result = subprocess.run(["data-toolset", "query", file_path, query], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ''
    # @TODO: check the result


def test_merge_command():
    file_paths = [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata2.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata3.parquet",
    ]
    output_path = Path("output.avro")
    try:
        result = subprocess.run(["data-toolset", "merge"] + file_paths + [output_path],
                                capture_output=True,
                                text=True)
        assert result.returncode == 0
        assert result.stderr == ''
        merged_data = pq.read_table(output_path)
        assert len(merged_data) == 3000
    finally:
        output_path.unlink()
