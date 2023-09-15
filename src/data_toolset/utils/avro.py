import os
import csv
import json
import logging
import typing as T
from pathlib import Path

import fastavro
import pyarrow as pa

from data_toolset.utils.base import BaseUtils


class AvroUtils(BaseUtils):
    @classmethod
    def to_arrow_table(cls, file_path: Path) -> pa.Table:
        """
        Read a Avro file and convert it into an Arrow Table.

        :param file_path: Path to the Parquet file to read.
        :type file_path: Path
        :return: Arrow Table containing the data from the Parquet file.
        :rtype: pa.Table
        """
        with open(file_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            table = pa.Table.from_pylist(list(avro_reader))
            return table

    @classmethod
    def write_arrow_table(cls, table: pa.Table, file_path: Path) -> None:
        """
        Write an Arrow Table to an Avro file.

        :param table: Arrow Table to write to the Avro file.
        :type table: pa.Table
        :param file_path: Path to the Avro file to write.
        :type file_path: Path
        """
        with open(file_path, "wb") as f:
            fastavro.writer(f, table.schema, table.to_pydict().values())

    @classmethod
    def validate_format(cls, file_path: Path) -> None:
        """
        Validate a Avro file.

        :param file_path: Path to the Avro file to validate.
        :type file_path: Path
        :raises Exception: If the file is invalid or doesn't meet the Avro format requirements.
        """
        # Check that the file exists and is not empty
        if not file_path.exists() or file_path.stat().st_size == 0:
            raise Exception()
        # Check that the file starts with the Avro magic bytes
        with open(file_path, "rb") as f:
            if f.read(4) != b"Obj\x01":
                raise Exception()

    @classmethod
    def meta(cls, file_path: Path) -> T.Tuple:
        """
        Inspect metadata of an Avro file.
        """
        with open(file_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            schema = avro_reader.writer_schema
            serialized_size = os.path.getsize(file_path)

            cls.print_metadata(schema, avro_reader.metadata, avro_reader.codec, serialized_size)
            return schema, avro_reader.metadata, avro_reader.codec, serialized_size

    @classmethod
    def schema(cls, file_path: Path) -> None:
        """
        Print the schema of an Avro file.

        :param file_path: Path to the Avro file to print the schema of.
        :type file_path: Path
        """
        with open(file_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            schema = avro_reader.writer_schema
            print(schema)

    @classmethod
    def stats(cls, file_path: Path) -> T.Tuple[int, T.Dict]:
        """
        Calculate statistics for an Avro file.

        :param file_path: Path to the Avro file to calculate statistics for.
        :type file_path: Path
        :return: A tuple containing the number of rows and column statistics.
        :rtype: Tuple[int, dict]
        """
        with open(file_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            num_rows = 0
            column_stats = {}
            for row in avro_reader:
                num_rows += 1
                for k, v in row.items():
                    column_stat = column_stats.get(k, {
                        "count": 0,
                        "null_count": 0,
                        "min": None,
                        "max": None
                    })
                    column_stat["count"] += 1
                    if v is None:
                        column_stat["null_count"] += 1
                    elif column_stat["min"] is None or cls.has_comparison_methods(v) and v < column_stat["min"]:
                        column_stat["min"] = v
                    elif column_stat["max"] is None or cls.has_comparison_methods(v) and v > column_stat["max"]:
                        column_stat["max"] = v
                    column_stats[k] = column_stat
            print(json.dumps(column_stats, indent=4))
            return num_rows, column_stats

    @classmethod
    def tail(cls, file_path: Path, n: int = 20) -> T.List:
        """
        Print the last N records of an Avro file.

        :param file_path: Path to the Avro file to read.
        :type file_path: Path
        :param n: Number of records to print from the end of the file.
        :type n: int
        :return: Arrow Table containing the last N records.
        :rtype: pa.Table
        """
        with open(file_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            records = list(avro_reader)
            num_records = len(records)

            # Calculate the number of records to read
            num_to_read = min(num_records, n)
            # Read the last N records and store them in a list
            records = [record for record in records[num_records - num_to_read:num_records]]
            print(records)
            return records

    @classmethod
    def head(cls, file_path: Path, n: int = 20) -> list:
        """
        Print the first N records of a Parquet file.

        :param file_path: Path to the Parquet file to read.
        :type file_path: Path
        :param n: Number of records to print from the beginning of the file.
        :type n: int
        :return: Arrow Table containing the first N records.
        :rtype: pa.Table
        """
        records = []
        with open(file_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            for i, record in enumerate(avro_reader):
                if i == n:
                    break
                print(record)
                records.append(record)
        return records

    @classmethod
    def count(cls, file_path: Path) -> int:
        """
        Count the number of records in an Avro file.

        :param file_path: Path to the Avro file to count records in.
        :type file_path: Path
        :return: The total number of records in the file.
        :rtype: int
        """
        with open(file_path, "rb") as f:
            record_count = 0
            avro_reader = fastavro.reader(f)
            for _ in avro_reader:
                record_count += 1

            print(record_count)
        return record_count

    @classmethod
    def merge(cls, file_paths: T.List[Path], output_path: Path) -> None:
        """
        Merge multiple Avro files into a single file.

        :param file_paths: List of file paths to merge.
        :type file_paths: List[Path]
        :param output_path: Path to the output merged file.
        :type output_path: Path
        """
        with open(output_path, mode="wb") as out:
            with open(file_paths[0], "rb") as f:
                avro_reader = fastavro.reader(f)
                schema = avro_reader.writer_schema
                fastavro.writer(out, schema, avro_reader, codec=avro_reader.codec, metadata=avro_reader.metadata)

        with open(output_path, mode="a+b") as out:
            for file_path in file_paths[1:]:
                with open(file_path, mode="rb") as f:
                    avro_reader = fastavro.reader(f)
                    fastavro.writer(out, None, avro_reader, codec=avro_reader.codec, metadata=avro_reader.metadata)

    @classmethod
    def validate(cls, file_path: Path, schema_path: Path = None) -> None:
        """
        Validate an Avro file against a given schema.

        :param file_path: Path to the Avro file to validate.
        :type file_path: Path
        :param schema_path: Path to the JSON schema file for validation.
        :type schema_path: Path
        """
        cls.validate_format(file_path)

        if schema_path:
            with open(schema_path, "r") as f:
                schema = json.load(f)

            with open(file_path, "rb") as f:
                avro_reader = fastavro.reader(f)
                try:
                    fastavro.validation.validate_many(avro_reader, schema=schema)
                    print("File validation successful.")
                    logging.info("File validation successful.")
                except fastavro.schema.SchemaParseException as e:
                    print(f"File validation failed: {str(e)}")
                    logging.error(f"File validation failed: {str(e)}")

        else:
            print("File is a valid Avro file.")
            logging.info("File is a valid Avro file.")

    @classmethod
    def to_json(cls, file_path: Path, output_path: Path) -> None:
        """
        Convert an Avro file to a JSON file.

        :param file_path: Path to the Avro file to convert.
        :type file_path: Path
        :param output_path: Path to the output JSON file.
        :type output_path: Path
        """
        with open(file_path, "rb") as f:
            data = list(fastavro.reader(f))
            with open(output_path, "w") as json_file:
                json.dump(data, json_file, sort_keys=True, indent=4)

    @classmethod
    def to_csv(cls, file_path: Path, output_path: Path, delimiter=",") -> None:
        """
        Convert an Avro file to a CSV file.

        :param file_path: Path to the Avro file to convert.
        :type file_path: Path
        :param output_path: Path to the CSV output file.
        :type output_path: Path
        :param delimiter: The delimiter character used in the CSV file (default is comma).
        :type delimiter: str
        """
        with open(file_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            avro_data = list(avro_reader)

        if avro_data:
            column_names = list(avro_data[0].keys())
        else:
            column_names = []

        with open(output_path, "w", newline="") as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=column_names, delimiter=delimiter)
            csv_writer.writeheader()
            csv_writer.writerows(avro_data)
