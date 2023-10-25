import typing as T
from abc import ABC, abstractmethod
from pathlib import Path

import duckdb
import polars
import pyarrow as pa


class BaseUtils(ABC):
    @classmethod
    def has_comparison_methods(cls, obj: T.Dict) -> bool:
        try:
            # @TODO: better solution is needed here
            return obj < obj < obj == obj
        except TypeError:
            return False

    @staticmethod
    def print_metadata(schema: T.Any, metadata: T.Any, codec: T.Any, serialized_size: T.Any) -> None:
        print(f"Schema: {schema}")
        print(f"Metadata: {metadata}")
        print(f"Codec: {codec}")
        print(f"Serialized size: {serialized_size}")

    @classmethod
    @abstractmethod
    def to_arrow_table(cls, file_path: Path) -> pa.Table:
        ...

    @classmethod
    @abstractmethod
    def validate_format(cls, file_path: Path) -> None:
        ...

    @classmethod
    @abstractmethod
    def meta(cls, file_path: Path) -> T.Tuple:
        ...

    @classmethod
    @abstractmethod
    def schema(cls, file_path: Path) -> None:
        ...

    @classmethod
    @abstractmethod
    def stats(cls, file_path: Path) -> T.Tuple[int, dict]:
        ...

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
        offset = 0 if table.num_rows - n < 0 else table.num_rows - n
        df = polars.from_arrow(table.slice(offset=offset, length=n))
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
        df = polars.from_arrow(table.slice(length=n))
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
        num_rows = table.num_rows
        print(num_rows)
        return num_rows

    @classmethod
    @abstractmethod
    def merge(cls, file_paths: T.List[Path], output_path: Path) -> None:
        ...

    @classmethod
    @abstractmethod
    def validate(cls, file_path: Path, schema_path: T.Optional[Path] = None) -> None:
        ...

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
        df.write_csv(file=output_path, has_header=has_header, separator=delimiter, line_terminator=line_terminator,
                     quote_char=quote)

    @classmethod
    def to_avro(cls, file_path: Path, output_path: Path,
                compression: T.Literal["uncompressed", "snappy", "deflate"] = "uncompressed") -> None:
        pass

    @classmethod
    def to_parquet(cls, file_path: Path, output_path: Path,
                   compression: T.Literal[
                       "lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd"] = "uncompressed") -> None:
        pass

    @classmethod
    def query(cls, file_path: Path, query_expression: str, *, chunk_size: int = 1000000) -> T.Union[polars.DataFrame, polars.Series]:
        """
        Query and filter data in an Avro or Parquet file using SQL-like expressions.

        :param file_path: Path to the Avro or Parquet file to query.
        :type file_path: Path
        :param query_expression: SQL-like query expression to filter and select data.
        :type query_expression: str
        :param chunk_size: Size of data chunks to retrieve per query iteration (default is 1,000,000 rows).
        :type chunk_size: int
        :return: Polars DataFrame containing the result of the query.
        :rtype: T.Union[polars.DataFrame, polars.Series]

        This class method allows you to run SQL-like queries on an Avro or Parquet file,
        filtering and selecting data based on the provided query expression.

        Example query expressions:
        - "SELECT * FROM 'weather.avro'" (selects all rows)
        - "SELECT temperature, humidity FROM 'weather.avro' WHERE temperature > 25" (selects specific columns and applies a filter)

        The method retrieves data in chunks to optimize memory usage, and the result is returned as an Arrow Table.
        """
        my_arrow_table = cls.to_arrow_table(file_path)

        con = duckdb.connect()
        con.register(file_path.name, my_arrow_table)

        # Run query that selects part of the data
        query = con.execute(query_expression)
        record_batch_reader = query.fetch_record_batch(rows_per_batch=chunk_size)

        # Retrieve all batch chunks
        all_chunks = []
        while True:
            try:
                chunk = record_batch_reader.read_next_batch()
                all_chunks.append(chunk)
            except StopIteration:
                break
        table = pa.Table.from_batches(batches=all_chunks)
        df = polars.from_arrow(table)
        print(df)
        return df

    @classmethod
    @abstractmethod
    def random_sample(cls, file_path: Path, output_path: Path, *, n: T.Optional[int] = None,
                      fraction: T.Optional[float] = None) -> None:
        ...
