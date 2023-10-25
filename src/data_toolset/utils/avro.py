import os
import json
import logging
import typing as T
from pathlib import Path

import fastavro
import polars
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
    def stats(cls, file_path: Path) -> polars.DataFrame:
        """
        Calculate statistics for an Avro file.

        :param file_path: Path to the Avro file to calculate statistics for.
        :type file_path: Path
        :return: A tuple containing the number of rows and column statistics.
        :rtype: polars.DataFrame
        """
        table = cls.to_arrow_table(file_path)
        df = polars.from_arrow(table)
        column_stats = df.describe()
        print(column_stats)
        return column_stats

    @classmethod
    def tail(cls, file_path: Path, n: int = 20) -> polars.DataFrame:
        """
        Print the last N records of an Avro file.

        :param file_path: Path to the Avro file to read.
        :type file_path: Path
        :param n: Number of records to print from the end of the file.
        :type n: int
        :return: Polars Dataframe containing the last N records.
        :rtype: polars.DataFrame
        """
        table = cls.to_arrow_table(file_path)
        df = polars.from_arrow(table)
        df = df.tail(n)
        print(df)
        return df

    @classmethod
    def head(cls, file_path: Path, n: int = 20) -> polars.DataFrame:
        """
        Print the first N records of a Parquet file.

        :param file_path: Path to the Parquet file to read.
        :type file_path: Path
        :param n: Number of records to print from the beginning of the file.
        :type n: int
        :return: Polars Dataframe containing the first N records.
        :rtype: polars.DataFrame
        """
        table = cls.to_arrow_table(file_path)
        df = polars.from_arrow(table)
        df = df.head(n)
        print(df)
        return df

    @classmethod
    def count(cls, file_path: Path) -> int:
        """
        Count the number of records in an Avro file.

        :param file_path: Path to the Avro file to count records in.
        :type file_path: Path
        :return: The total number of records in the file.
        :rtype: int
        """
        table = cls.to_arrow_table(file_path)
        record_count = table.shape[0]
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
    def validate(cls, file_path: Path, schema_path: T.Optional[Path] = None) -> None:
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
    def to_json(cls, file_path: Path, output_path: Path, pretty: bool = False) -> None:
        """
        Convert an Avro file to a JSON file.

        :param file_path: Path to the Avro file to convert.
        :type file_path: Path
        :param output_path: Path to the output JSON file.
        :type output_path: Path
        :param pretty: Whether to format the JSON file with indentation (default is False).
        :type pretty: bool
        """
        table = cls.to_arrow_table(file_path)
        df = polars.from_arrow(table)
        df.write_json(file=output_path, pretty=pretty, row_oriented=True)

    @classmethod
    def to_csv(cls, file_path: Path, output_path: Path, has_header: bool = True, delimiter: str = ",",
               line_terminator: str = "\n", quote: str = '\"') -> None:
        """
        Convert an Avro file to a CSV file.

        :param file_path: Path to the Avro file to convert.
        :type file_path: Path
        :param output_path: Path to the output CSV file.
        :type output_path: Path
        :param has_header: Whether the CSV file should include a header row (default is True).
        :type has_header: bool
        :param delimiter: The character used to separate fields in the CSV (default is ',').
        :type delimiter: str
        :param line_terminator: The character(s) used to terminate lines in the CSV (default is '\n').
        :type line_terminator: str
        :param quote: The character used to enclose fields in quotes (default is '\"').
        :type quote: str
        """
        table = cls.to_arrow_table(file_path)
        df = polars.from_arrow(table)
        df.write_csv(file=output_path, has_header=has_header, separator=delimiter, line_terminator=line_terminator, quote=quote)

    @classmethod
    def to_parquet(cls, file_path: Path, output_path: Path,
                   compression: T.Literal[
                       "lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd"] = "uncompressed") -> None:
        """
        Convert an Avro file to a Parquet file.

        :param file_path: Path to the Avro file to convert.
        :type file_path: Path
        :param output_path: Path to the output Parquet file.
        :type output_path: Path
        :param compression: The compression method to use for the Parquet file (default is 'uncompressed').
        :type compression: str
        """
        table = cls.to_arrow_table(file_path)
        df = polars.from_arrow(table)
        df.write_parquet(file=output_path)

    @classmethod
    def random_sample(cls, file_path: Path, output_path: Path, n: T.Optional[int] = None, fraction: T.Optional[float] = None,
                      with_replacement: bool = False, shuffle: bool = False) -> None:
        """
        Create a random sample from an Avro file and save it as an Avro file.

        :param file_path: Path to the Avro file to sample from.
        :type file_path: Path
        :param output_path: Path to the output Avro file for the random sample.
        :type output_path: Path
        :param n: The number of records to include in the random sample.
        :type n: int
        :param fraction: The fraction of records to include in the random sample (alternative to 'n').
        :type fraction: float
        :param with_replacement: Whether to sample with replacement (default is False).
        :type with_replacement: bool
        :param shuffle: Whether to shuffle the input data before sampling (default is False).
        :type shuffle: bool
        """
        table = cls.to_arrow_table(file_path)
        df = polars.from_arrow(table)
        sample_df = df.sample(n=n, fraction=fraction, with_replacement=with_replacement, shuffle=shuffle)
        sample_df.write_avro(output_path)
