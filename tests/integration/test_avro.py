import subprocess
from pathlib import Path

import fastavro
import pytest
from utils import TEST_DATA_DIR


# @TODO: consolidate file paths somewhere
@pytest.mark.parametrize(
    ("file_path", "num_records"),
    [
        (TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro", "0"),
        (TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro", "10"),
    ],
)
def test_head_command(file_path, num_records):
    result = subprocess.run(["data-toolset", "head", file_path, "-n", num_records], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ""
    assert len(result.stdout) > 0


@pytest.mark.parametrize(
    ("file_path", "num_records"),
    [
        (TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro", "0"),
        (TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro", "10"),
    ],
)
def test_tail_command(file_path, num_records):
    result = subprocess.run(["data-toolset", "tail", file_path, "-n", num_records], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ""
    assert len(result.stdout) > 0


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro",
    ],
)
def test_meta_command(file_path):
    result = subprocess.run(["data-toolset", "meta", file_path], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ""
    # @TODO: check the result
    assert len(result.stdout) > 0


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro",
    ],
)
def test_schema_command(file_path):
    result = subprocess.run(["data-toolset", "schema", file_path], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ""
    # @TODO: check the result
    assert len(result.stdout) > 0


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro",
    ],
)
def test_stats_command(file_path):
    result = subprocess.run(["data-toolset", "stats", file_path], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ""
    # @TODO: check the result
    assert len(result.stdout) > 0


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro",
    ],
)
def test_validate_command(file_path):
    schema_path = TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata.avsc"

    result = subprocess.run(["data-toolset", "validate", file_path, "--schema_path", schema_path], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout == 'File validation successful.\n'


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro",
    ],
)
def test_query_command(file_path):
    query = f"select first_name from '{file_path.name}' where gender='Female'"

    result = subprocess.run(["data-toolset", "query", file_path, query], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ""
    # @TODO: check the result


def test_merge_command():
    file_paths = [
        TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro",
        TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata2.avro",
        TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata3.avro",
    ]
    output_path = Path("output.avro")
    try:
        result = subprocess.run(["data-toolset", "merge"] + file_paths + [output_path],
                                capture_output=True,
                                text=True)
        assert result.returncode == 0
        assert result.stderr == ""
        assert output_path.is_file()
        assert output_path.stat().st_size > 0
        merged_data = []
        with open(output_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            for record in avro_reader:
                merged_data.append(record)

        assert len(merged_data) == 2998
    finally:
        output_path.unlink()


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro",
    ],
)
def test_count_command(file_path):
    result = subprocess.run(["data-toolset", "count", file_path], capture_output=True,
                            text=True)
    assert result.returncode == 0
    assert result.stderr == ""
    # @TODO: check the result


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro",
    ],
)
def test_to_json_command(file_path):
    output_path = Path("output.json")
    try:
        result = subprocess.run(["data-toolset", "to_json", file_path, output_path], capture_output=True,
                                text=True)
        assert result.returncode == 0
        assert result.stderr == ""
        assert output_path.is_file()
        assert output_path.stat().st_size > 0
        # @TODO: check the result
    finally:
        output_path.unlink()


@pytest.mark.parametrize(
    "file_path",
    [
        TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro",
    ],
)
def test_to_csv_command(file_path):
    output_path = Path("output.csv")
    try:
        result = subprocess.run(["data-toolset", "to_csv", file_path, output_path], capture_output=True,
                                text=True)
        assert result.returncode == 0
        assert result.stderr == ""
        assert output_path.is_file()
        assert output_path.stat().st_size > 0
        # @TODO: check the result
    finally:
        output_path.unlink()


@pytest.mark.parametrize(
    ("file_path", "compression"),
    [
        (TEST_DATA_DIR / "data" / "avro" / "test.avro", "uncompressed"),
        (TEST_DATA_DIR / "data" / "avro" / "test.avro", "snappy"),
        (TEST_DATA_DIR / "data" / "avro" / "test.avro", "gzip"),
        (TEST_DATA_DIR / "data" / "avro" / "test.avro", "lz4"),
        (TEST_DATA_DIR / "data" / "avro" / "test.avro", "lzo"),
        (TEST_DATA_DIR / "data" / "avro" / "test.avro", "brotli"),
        (TEST_DATA_DIR / "data" / "avro" / "test.avro", "zstd"),
    ],
)
def test_to_parquet_command(file_path, compression):
    output_path = Path("output.parquet")
    try:
        result = subprocess.run([
            "data-toolset", "to_parquet", file_path, output_path, "--compression", compression],
            capture_output=True,
            text=True)
        assert result.returncode == 0
        assert result.stderr == ""
        assert output_path.is_file()
        assert output_path.stat().st_size > 0
    finally:
        output_path.unlink()


@pytest.mark.parametrize(
    ("file_path", "num_records"),
    [
        (TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro", "0"),
        (TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro", "10"),
    ],
)
def test_random_sample_command(file_path, num_records):
    output_path = Path("output.avro")
    result = subprocess.run([
        "data-toolset", "random_sample", file_path, output_path, "--n", num_records], capture_output=True,
        text=True)
    assert result.returncode == 0
    assert result.stderr == ""
    assert output_path.is_file()
    assert output_path.stat().st_size > 0
