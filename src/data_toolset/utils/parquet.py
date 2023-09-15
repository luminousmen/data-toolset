import csv
import json
import logging
import typing as T
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from data_toolset.utils.base import BaseUtils
from data_toolset.utils.utils import NpEncoder


class ParquetUtils(BaseUtils):
    @classmethod
    def to_arrow_table(cls, file_path: Path) -> pa.Table:
        """
        Read a Parquet file and convert it into an Arrow Table.

        :param file_path: Path to the Parquet file to read.
        :type file_path: Path
        :return: Arrow Table containing the data from the Parquet file.
        :rtype: pa.Table
        """
        table = pa.parquet.read_table(file_path)
        return table

    @classmethod
    def write_arrow_table(cls, table: pa.Table, file_path: Path) -> None:
        """
        Write an Arrow Table to a Parquet file.

        :param table: Arrow Table to write to the Parquet file.
        :type table: pa.Table
        :param file_path: Path to the Parquet file to write.
        :type file_path: Path
        """
        pa.parquet.write_table(table, file_path)

    @classmethod
    def validate_format(cls, file_path: Path) -> None:
        """
        Validate a Parquet file.

        :param file_path: Path to the Parquet file to validate.
        :type file_path: Path
        :raises Exception: If the file is invalid or doesn't meet the Parquet format requirements.
        """
        # Check that the file exists and is not empty
        if not file_path.exists() or file_path.stat().st_size == 0:
            raise Exception()
        # Check that the file starts with the Parquet magic bytes
        with open(file_path, "rb") as f:
            if f.read(4) != b"PAR1":
                raise Exception()

    @classmethod
    def meta(cls, file_path: Path) -> T.Tuple:
        """
        Inspect metadata of a Parquet file.

        :param file_path: Path to the Parquet file to inspect.
        :type file_path: Path
        :return: A tuple containing schema, metadata, codec, and metadata.
        :rtype: Tuple[pyarrow.Schema, pyarrow.parquet.FileMetadata, str, pyarrow.parquet.FileMetadata]
        """
        parquet_file = pq.ParquetFile(file_path)
        codec = parquet_file.metadata.row_group(0).column(0).compression
        cls.print_metadata(parquet_file.schema, parquet_file.metadata, codec, parquet_file.metadata)
        return parquet_file.schema, parquet_file.metadata, codec, parquet_file.metadata

    @classmethod
    def schema(cls, file_path: Path) -> None:
        """
        Print the schema of a Parquet file.

        :param file_path: Path to the Parquet file to print the schema of.
        :type file_path: Path
        """
        parquet_file = pq.ParquetFile(file_path)
        print(parquet_file.schema)

    @classmethod
    def stats(cls, file_path: Path) -> T.Tuple[int, dict]:
        """
        Calculate statistics for a Parquet file.

        :param file_path: Path to the Parquet file to calculate statistics for.
        :type file_path: Path
        :return: A tuple containing the number of rows and column statistics.
        :rtype: Tuple[int, dict]
        """
        parquet_file = pq.ParquetFile(file_path)
        num_rows = parquet_file.metadata.num_rows
        column_stats = {}
        for i in range(parquet_file.num_row_groups):
            table = parquet_file.read_row_group(i)
            for j, column_name in enumerate(table.schema.names):
                column = table.column(j)
                column_stat = column_stats.get(column_name, {
                    "count": 0,
                    "null_count": 0,
                    "min": None,
                    "max": None
                })
                column_stat["count"] += len(column)
                column_stat["null_count"] += column.null_count
                # Process each chunk in the ChunkedArray
                for chunk in column.iterchunks():
                    if chunk.null_count == len(chunk):  # Skip if all values are null
                        continue
                    non_null_values = chunk.to_pandas().dropna().values

                    if len(non_null_values) > 0:
                        chunk_min = non_null_values[0]
                        chunk_max = non_null_values[-1]

                        if column_stat["min"] is None or chunk_min < column_stat["min"]:
                            column_stat["min"] = chunk_min

                        if column_stat["max"] is None or chunk_max > column_stat["max"]:
                            column_stat["max"] = chunk_max

                column_stats[column_name] = column_stat

        print(json.dumps(column_stats, indent=4, cls=NpEncoder, default=str))
        return num_rows, column_stats

    @classmethod
    def tail(cls, file_path: Path, n: int = 20) -> pa.Table:
        """
        Print the last N records of a Parquet file.

        :param file_path: Path to the Parquet file to read.
        :type file_path: Path
        :param n: Number of records to print from the end of the file.
        :type n: int
        :return: Arrow Table containing the last N records.
        :rtype: pa.Table
        """
        table = pq.read_table(str(file_path), use_threads=True)
        total_rows = table.num_rows
        start_row = max(total_rows - n, 0)
        cls._print_table(table.slice(start_row, total_rows))
        return table

    @classmethod
    def head(cls, file_path: Path, n: int = 20) -> pa.Table:
        """
        Print the first N records of a Parquet file.

        :param file_path: Path to the Parquet file to read.
        :type file_path: Path
        :param n: Number of records to print from the beginning of the file.
        :type n: int
        :return: Arrow Table containing the first N records.
        :rtype: pa.Table
        """
        table = pa.parquet.read_table(file_path).slice(0, n)
        cls._print_table(table)
        return table

    @classmethod
    def count(cls, file_path: Path) -> int:
        """
        Count the number of records in a Parquet file.

        :param file_path: Path to the Parquet file to count records in.
        :type file_path: Path
        :return: The total number of records in the file.
        :rtype: int
        """
        parquet_file = pq.ParquetFile(file_path)
        num_rows = parquet_file.metadata.num_rows
        print(num_rows)
        return num_rows

    @classmethod
    def merge(cls, file_paths: T.List[Path], output_path: Path) -> None:
        """
        Merge multiple Parquet files into a single file.

        :param file_paths: List of file paths to merge.
        :type file_paths: List[Path]
        :param output_path: Path to the output merged file.
        :type output_path: Path
        """
        tables = []
        for file_path in file_paths:
            table = pq.read_table(file_path)
            tables.append(table)

        table = pa.concat_tables(tables)
        pq.write_table(table, output_path)

    @classmethod
    def validate(cls, file_path: Path, schema_path: Path = None) -> None:
        """
        Validate a Parquet file against a given schema.

        :param file_path: Path to the Parquet file to validate.
        :type file_path: Path
        :param schema_path: Path to the JSON schema file for validation.
        :type schema_path: Path

        This class method validates the specified Parquet file against a given JSON schema.
        It checks if the file adheres to the schema and prints the validation result.
        """
        cls.validate_format(file_path)

        if schema_path:
            # @TODO: implement that
            pass
        else:
            print("File is a valid Parquet file.")
            logging.info("File is a valid Parquet file.")

    @classmethod
    def to_json(cls, file_path: Path, output_path: Path) -> None:
        """
        Convert an Parquet file to a JSON file.

        :param file_path: Path to the Parquet file to convert.
        :type file_path: Path
        :param output_path: Path to the output JSON file.
        :type output_path: Path
        """
        table = cls.to_arrow_table(file_path)
        json_records = json.loads(table.to_pandas().to_json(orient="records"))
        with output_path.open(mode="w") as output_file:
            json.dump(json_records, output_file, sort_keys=True, indent=4, cls=NpEncoder)

    @classmethod
    def to_csv(cls, file_path: Path, output_path: Path, delimiter=",") -> None:
        """
        Convert an Parquet file to a CSV file.

        :param file_path: Path to the Parquet file to convert.
        :type file_path: Path
        :param output_path: Path to the output CSV file.
        :type output_path: Path
        :param delimiter: The delimiter character used in the CSV file (default is comma).
        :type delimiter: str
        """

        # @TODO(kirillb): make it better - dumb solution
        table = cls.to_arrow_table(file_path)
        schema = table.schema.names
        json_records = json.loads(table.to_pandas().to_json(orient="records"))

        with open(output_path, "w", newline="") as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=schema, delimiter=delimiter)
            csv_writer.writeheader()
            csv_writer.writerows(json_records)
