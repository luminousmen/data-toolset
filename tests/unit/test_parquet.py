import csv
import json
from pathlib import Path

import polars
import pyarrow.parquet as pq
import pytest
from utils import TEST_DATA_DIR, DATA_JSON_EXPECTED, DATA_CSV_EXPECTED

from data_toolset.utils.parquet import ParquetUtils


def test_validate__valid():
    file_path = TEST_DATA_DIR / "data" / "parquet" / "test-snappy.parquet"
    ParquetUtils.validate(file_path)


def test_validate__empty():
    temp_file = Path("empty_file.parquet")
    temp_file.touch()
    try:
        with pytest.raises(Exception):
            ParquetUtils.validate(temp_file)
    finally:
        temp_file.unlink()


def test_validate__bad_format():
    temp_file = Path("file.txt")
    temp_file.touch()
    with open(temp_file, "w") as file:
        file.write("Test")

    try:
        with pytest.raises(Exception):
            ParquetUtils.validate(temp_file)
    finally:
        temp_file.unlink()


def test_validate__not_exist():
    temp_file = Path("non_existent_file.parquet")
    try:
        with pytest.raises(Exception):
            ParquetUtils.validate(temp_file)
    finally:
        pass


def test_snappy_meta():
    file_path = TEST_DATA_DIR / "data" / "parquet" / "test-snappy.parquet"
    result = ParquetUtils.meta(file_path)

    # Add assertions to verify the expected output
    assert isinstance(result, tuple)
    assert len(result) == 4
    assert result[2] == "SNAPPY"
    assert result[3].serialized_size == 4420
    assert result[3].num_columns == 8
    assert result[3].num_rows == 3


def test_gzip_meta():
    file_path = TEST_DATA_DIR / "data" / "parquet" / "test-gzip.parquet"
    result = ParquetUtils.meta(file_path)

    # Add assertions to verify the expected output
    assert isinstance(result, tuple)
    assert len(result) == 4
    assert result[2] == "GZIP"
    assert result[3].serialized_size == 4421
    assert result[3].num_columns == 8
    assert result[3].num_rows == 3


def test_stats():
    file_path = TEST_DATA_DIR / "data" / "parquet" / "test.parquet"
    result = ParquetUtils.stats(file_path)
    assert result.shape[0] == 9
    assert result.shape[1] == 7 + 1


def test_head():
    n = 3
    file_path = TEST_DATA_DIR / "data" / "parquet" / "test.parquet"
    result = ParquetUtils.head(file_path, n)
    assert isinstance(result, polars.DataFrame)
    assert len(result) == min(n, len(result))


def test_tail_function():
    n = 3
    file_path = TEST_DATA_DIR / "data" / "parquet" / "test.parquet"
    result = ParquetUtils.tail(file_path, n)
    assert isinstance(result, polars.DataFrame)
    assert len(result) == min(n, len(result))


def test_count():
    file_path = TEST_DATA_DIR / "data" / "parquet" / "test.parquet"
    result = ParquetUtils.count(file_path)
    assert result == 3


def test_merge():
    file_path = TEST_DATA_DIR / "data" / "parquet" / "test.parquet"
    temp_file = Path("empty_file.avro")
    try:
        ParquetUtils.merge([file_path, file_path], temp_file)
        merged_data = pq.read_table(temp_file)
        assert len(merged_data) == 6
    finally:
        temp_file.unlink()


def test_schema():
    pass


def test_query():
    pass


def test_to_json():
    file_path = TEST_DATA_DIR / "data" / "parquet" / "test.parquet"
    temp_file = Path("data.json")
    try:
        ParquetUtils.to_json(file_path, temp_file)

        with temp_file.open(mode="r", encoding="utf-8") as output_file:
            json_data = json.load(output_file)

        assert json_data == DATA_JSON_EXPECTED
    finally:
        temp_file.unlink()


def test_to_csv():
    file_path = TEST_DATA_DIR / "data" / "sample-data" / "parquet" / "userdata1.parquet"
    temp_file = Path("data.csv")
    try:
        ParquetUtils.to_csv(file_path, temp_file)

        with temp_file.open(mode="r", encoding="utf-8") as output_file:
            csv_reader = csv.DictReader(output_file)
            csv_data = list(csv_reader)

        # @TODO(kirillb): assert the content
        # assert csv_data == DATA_CSV_EXPECTED
    finally:
        temp_file.unlink()


def test_to_avro():
    file_path = TEST_DATA_DIR / "data" / "parquet" / "test.parquet"
    temp_file = Path("data.avro")
    try:
        ParquetUtils.to_avro(file_path, temp_file)
        assert temp_file.is_file()
        assert temp_file.stat().st_size > 0
    finally:
        temp_file.unlink()
