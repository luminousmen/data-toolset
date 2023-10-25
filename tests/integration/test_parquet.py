import subprocess
from pathlib import Path

import pytest
from utils import TEST_DATA_DIR
import pyarrow.parquet as pq


# @TODO: consolidate file paths somewhere
@pytest.mark.parametrize(
    ("file_path", "num_records"),
    [
        (TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet", "0"),
        (TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet", "10"),
    ],
)
def test_head_command(file_path, num_records):
    result = subprocess.run(["data-toolset", "head", file_path, "-n", num_records], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ''
    assert len(result.stdout) > 0


@pytest.mark.parametrize(
    ("file_path", "num_records"),
    [
        (TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet", "0"),
        (TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet", "10"),
    ],
)
def test_tail_command(file_path, num_records):
    result = subprocess.run(["data-toolset", "tail", file_path, "-n", num_records], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ''
    assert len(result.stdout) > 0


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
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
    ],
)
def test_stats_command(file_path):
    result = subprocess.run(["data-toolset", "stats", file_path], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ''
    # @TODO: check the result
    assert len(result.stdout) > 0


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
    ],
)
def test_query_command(file_path):
    query = f"select first_name from '{file_path.name}' where gender='Female'"

    result = subprocess.run(["data-toolset", "query", file_path, query], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ''
    # @TODO: check the result


def test_validate_command():
    pass


def test_merge_command():
    file_paths = [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata2.parquet",
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata3.parquet",
    ]
    output_path = Path("output.parquet")
    try:
        result = subprocess.run(["data-toolset", "merge"] + file_paths + [output_path],
                                capture_output=True,
                                text=True)
        assert result.returncode == 0
        assert result.stderr == ''
        assert output_path.is_file()
        assert output_path.stat().st_size > 0
        merged_data = pq.read_table(output_path)
        assert len(merged_data) == 3000
    finally:
        output_path.unlink()


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
    ],
)
def test_count_command(file_path):
    result = subprocess.run(["data-toolset", "count", file_path], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ''
    # @TODO: check the result


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
    ],
)
def test_to_json_command(file_path):
    output_path = Path("output.json")
    try:
        result = subprocess.run([
            "data-toolset", "to_json", file_path, output_path], capture_output=True,
            text=True)
        assert result.returncode == 0
        assert result.stderr == ''
        assert output_path.is_file()
        assert output_path.stat().st_size > 0
    finally:
        output_path.unlink()


def test_to_csv_command():
    pass


@pytest.mark.parametrize(
    ("file_path", "compression"),
    [
        (TEST_DATA_DIR / "data" / "parquet" / "test.parquet", "uncompressed"),
        (TEST_DATA_DIR / "data" / "parquet" / "test.parquet", "snappy"),
        (TEST_DATA_DIR / "data" / "parquet" / "test.parquet", "deflate"),
    ],
)
def test_to_avro_command(file_path, compression):
    output_path = Path("output.avro")
    try:
        result = subprocess.run([
            "data-toolset", "to_avro", file_path, output_path, "--compression", compression],
            capture_output=True,
            text=True)
        assert result.returncode == 0
        assert result.stderr == ''
        assert output_path.is_file()
        assert output_path.stat().st_size > 0
    finally:
        output_path.unlink()


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet",
    ],
)
def test_random_sample_command(file_path):
    output_path = Path("output.parquet")
    result = subprocess.run([
        "data-toolset", "random_sample", file_path, output_path, "--n", "10"], capture_output=True,
        text=True)
    assert result.returncode == 0
    assert result.stderr == ''
    assert output_path.is_file()
    assert output_path.stat().st_size > 0
