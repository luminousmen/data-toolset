import argparse
from unittest.mock import patch

import pytest
from utils import TEST_DATA_DIR

from data_toolset.main import main


@pytest.mark.parametrize(
    "command, file_path",
    [
        ("head", TEST_DATA_DIR / "data" / "avro" / "test.avro"),
        ("tail", TEST_DATA_DIR / "data" / "avro" / "test.avro"),
        ("count", TEST_DATA_DIR / "data" / "avro" / "test.avro"),
        ("stats", TEST_DATA_DIR / "data" / "avro" / "test.avro"),
        ("schema", TEST_DATA_DIR / "data" / "avro" / "test.avro"),
        ("meta", TEST_DATA_DIR / "data" / "avro" / "test.avro"),
        ("validate", TEST_DATA_DIR / "data" / "avro" / "test.avro"),
    ],
)
def test_main__avro_with_valid_command(command, file_path):
    with patch("data_toolset.utils.avro.AvroUtils." + command) as mock_head:
        with patch("argparse.ArgumentParser.parse_args",
                   return_value=argparse.Namespace(command=command, file_path=file_path)):
            main()
            mock_head.assert_called_once_with(file_path)


@pytest.mark.parametrize(
    "command, file_path",
    [
        ("head", TEST_DATA_DIR / "data" / "parquet" / "test.parquet"),
        ("tail", TEST_DATA_DIR / "data" / "parquet" / "test.parquet"),
        ("count", TEST_DATA_DIR / "data" / "parquet" / "test.parquet"),
        ("stats", TEST_DATA_DIR / "data" / "parquet" / "test.parquet"),
        ("schema", TEST_DATA_DIR / "data" / "parquet" / "test.parquet"),
        ("meta", TEST_DATA_DIR / "data" / "parquet" / "test.parquet"),
        ("validate", TEST_DATA_DIR / "data" / "parquet" / "test.parquet"),
    ],
)
def test_main__parquet_with_valid_command(command, file_path):
    with patch("data_toolset.utils.parquet.ParquetUtils." + command) as mock_head:
        with patch("argparse.ArgumentParser.parse_args",
                   return_value=argparse.Namespace(command=command, file_path=file_path)):
            main()
            mock_head.assert_called_once_with(file_path)


@pytest.mark.parametrize(
    "command, file_path",
    [
        ("invalid", TEST_DATA_DIR / "data" / "avro" / "test.avro"),
        ("", TEST_DATA_DIR / "data" / "avro" / "test.avro"),
    ],
)
def test_main__with_invalid_command(command, file_path):
    with pytest.raises(ValueError, match="Invalid command."):
        with patch("argparse.ArgumentParser.parse_args",
                   return_value=argparse.Namespace(command=command, file_path=file_path)):
            main()


@pytest.mark.parametrize(
    "command, file_path",
    [
        ("head", TEST_DATA_DIR / "data" / "avro" / "test.csv"),
        ("head", TEST_DATA_DIR / "data" / "avro" / "test.json"),
        ("head", TEST_DATA_DIR / "data" / "avro" / "test.txt"),
    ],
)
def test_main__unsupported_format(command, file_path):
    with pytest.raises(ValueError, match="Unsupported file format."):
        with patch("argparse.ArgumentParser.parse_args",
                   return_value=argparse.Namespace(command=command, file_path=file_path)):
            main()
