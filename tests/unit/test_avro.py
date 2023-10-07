import csv
import json
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import fastavro
import polars
import pytest
from utils import TEST_DATA_DIR, DATA_JSON_EXPECTED, DATA_CSV_EXPECTED

from data_toolset.utils.avro import AvroUtils


def test_validate():
    file_path = TEST_DATA_DIR / "data" / "avro" / "test.avro"
    captured_output = StringIO()
    with patch("sys.stdout", captured_output):
        AvroUtils.validate(file_path)

    captured_content = captured_output.getvalue()
    assert captured_content.strip(), "File is a valid Avro file."


def test_validate__empty():
    temp_file = Path("empty_file.avro")
    temp_file.touch()
    try:
        with pytest.raises(Exception):
            AvroUtils.validate(temp_file)
    finally:
        temp_file.unlink()


def test_validate__bad_format():
    temp_file = Path("file.txt")
    temp_file.touch()
    with open(temp_file, "w") as file:
        file.write("Test")

    try:
        with pytest.raises(Exception):
            AvroUtils.validate(temp_file)
    finally:
        temp_file.unlink()


def test_validate__not_exist():
    temp_file = Path("non_existent_file.avro")
    try:
        with pytest.raises(Exception):
            AvroUtils.validate(temp_file)
    finally:
        pass


def test_validate__with_valid_schema():
    file_path = TEST_DATA_DIR / "data" / "avro" / "test.avro"
    schema_path = TEST_DATA_DIR / "data" / "schema_valid.avsc"

    captured_output = StringIO()
    with patch("sys.stdout", captured_output):
        AvroUtils.validate(file_path, schema_path)

    captured_content = captured_output.getvalue()
    assert captured_content.strip(), "File validation successful."


def test_validate__with_invalid_schema():
    file_path = TEST_DATA_DIR / "data" / "avro" / "test.avro"
    schema_path = TEST_DATA_DIR / "data" / "schema_invalid.avsc"
    with pytest.raises(Exception):
        AvroUtils.validate(file_path, schema_path)


def test_snappy_meta():
    file_path = TEST_DATA_DIR / "data" / "avro" / "test-snappy.avro"
    result = AvroUtils.meta(file_path)
    expected_schema = {"fields": [{"name": "character", "type": "string"}, {"name": "age", "type": "int"},
                                  {"name": "is_human", "type": "boolean"}, {"name": "height", "type": "double"},
                                  {"name": "quote", "type": "string"},
                                  {"name": "friends", "type": {"items": "string", "type": "array"}},
                                  {"name": "appearance", "type": {"type": "map", "values": "string"}}],
                       "name": "AliceData", "type": "record"}

    # Add assertions to verify the expected output
    assert isinstance(result, tuple)
    assert len(result) == 4
    assert result[0] == expected_schema
    assert result[2] == "snappy"
    assert result[3] == 681


def test_deflate_meta():
    file_path = TEST_DATA_DIR / "data" / "avro" / "test-deflate.avro"
    result = AvroUtils.meta(file_path)
    expected_schema = {"fields": [{"name": "character", "type": "string"}, {"name": "age", "type": "int"},
                                  {"name": "is_human", "type": "boolean"}, {"name": "height", "type": "double"},
                                  {"name": "quote", "type": "string"},
                                  {"name": "friends", "type": {"items": "string", "type": "array"}},
                                  {"name": "appearance", "type": {"type": "map", "values": "string"}}],
                       "name": "AliceData", "type": "record"}

    # Add assertions to verify the expected output
    assert isinstance(result, tuple)
    assert len(result) == 4
    assert result[0] == expected_schema
    assert result[2] == "deflate"
    assert result[3] == 651


def test_meta():
    file_path = TEST_DATA_DIR / "data" / "avro" / "test.avro"
    result = AvroUtils.meta(file_path)
    expected_schema = {"fields": [{"name": "character", "type": "string"}, {"name": "age", "type": "int"},
                                  {"name": "is_human", "type": "boolean"}, {"name": "height", "type": "double"},
                                  {"name": "quote", "type": "string"},
                                  {"name": "friends", "type": {"items": "string", "type": "array"}},
                                  {"name": "appearance", "type": {"type": "map", "values": "string"}}],
                       "name": "AliceData", "type": "record"}

    # Add assertions to verify the expected output
    assert isinstance(result, tuple)
    assert len(result) == 4
    assert result[0] == expected_schema
    assert result[2] == "null"
    assert result[3] == 700


def test_stats():
    file_path = TEST_DATA_DIR / "data" / "avro" / "test.avro"

    # Call the stats method
    result = AvroUtils.stats(file_path)
    num_rows, columns_stats = result

    assert num_rows == 3
    assert isinstance(columns_stats, dict)
    for col, stats in columns_stats.items():
        assert isinstance(stats["count"], int)
        assert isinstance(stats["null_count"], int)
        assert "min" in stats
        assert "max" in stats


def test_head():
    n = 3
    file_path = TEST_DATA_DIR / "data" / "avro" / "test.avro"
    result = AvroUtils.head(file_path, n)
    assert isinstance(result, polars.DataFrame)
    assert len(result) == min(n, len(result))


def test_head__cropped_output():
    n = 1
    file_path = TEST_DATA_DIR / "data" / "avro" / "test.avro"
    result = AvroUtils.head(file_path, n)
    assert isinstance(result, polars.DataFrame)
    assert len(result) == min(n, len(result))


def test_tail():
    n = 3
    file_path = TEST_DATA_DIR / "data" / "avro" / "test.avro"
    result = AvroUtils.tail(file_path, n)
    assert isinstance(result, polars.DataFrame)
    assert len(result) == min(n, len(result))


def test_count():
    file_path = TEST_DATA_DIR / "data" / "avro" / "test.avro"
    result = AvroUtils.count(file_path)
    assert result == 3


def test_merge():
    file_path = TEST_DATA_DIR / "data" / "avro" / "test.avro"
    temp_file = Path("empty_file.avro")
    try:
        AvroUtils.merge([file_path, file_path], temp_file)
        merged_data = []
        with open(temp_file, "rb") as f:
            avro_reader = fastavro.reader(f)
            for record in avro_reader:
                merged_data.append(record)

        assert len(merged_data) == 6
    finally:
        temp_file.unlink()


def test_schema():
    pass


def test_query():
    pass


def test_to_json():
    file_path = TEST_DATA_DIR / "data" / "avro" / "test.avro"
    temp_file = Path("data.json")

    AvroUtils.to_json(file_path, temp_file)

    with temp_file.open(mode="r", encoding="utf-8") as output_file:
        json_data = json.load(output_file)

    assert json_data == DATA_JSON_EXPECTED


def test_to_csv():
    file_path = TEST_DATA_DIR / "data" / "sample-data" / "avro" / "userdata1.avro"
    temp_file = Path("data.csv")

    AvroUtils.to_csv(file_path, temp_file)

    with temp_file.open(mode="r", encoding="utf-8") as output_file:
        csv_reader = csv.DictReader(output_file)
        csv_data = list(csv_reader)

    # @TODO(kirillb): assert the content
    # assert csv_data == DATA_CSV_EXPECTED

