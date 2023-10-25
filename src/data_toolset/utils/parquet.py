import json
import logging
import typing as T
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import polars

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
                    non_null_values = polars.from_arrow(chunk).drop_nans()

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
    def validate(cls, file_path: Path, schema_path: T.Optional[Path] = None) -> None:
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
            df = polars.read_parquet(source=file_path)
            print(df.schema)
            # @TODO: implement that
        else:
            print("File is a valid Parquet file.")
            logging.info("File is a valid Parquet file.")

    @classmethod
    def to_avro(cls, file_path: Path, output_path: Path,
                compression: T.Literal["uncompressed", "snappy", "deflate"] = "uncompressed") -> None:
        """
        Convert an Parquet file to a Avro file.

        :param file_path: Path to the Avro file to convert.
        :type file_path: Path
        :param output_path: Path to the output Avro file.
        :type output_path: Path
        :param compression: The compression method to use for the Parquet file (default is 'uncompressed').
        :type compression: str
        """
        # @TODO(kirillb): not supporting timestamps at the moment
        df = polars.read_parquet(source=file_path)
        df.write_avro(file=output_path, compression=compression)

    @classmethod
    def random_sample(cls, file_path: Path, output_path: Path, n: T.Optional[int] = None, fraction: T.Optional[float] = None,
                      with_replacement: bool = False, shuffle: bool = False) -> None:
        """
        Create a random sample from an Parquet file and save it as an Parquet file.

        :param file_path: Path to the Parquet file to sample from.
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
        df = polars.read_parquet(source=file_path)
        sample_df = df.sample(n=n, fraction=fraction, with_replacement=with_replacement, shuffle=shuffle)
        sample_df.write_parquet(output_path)
