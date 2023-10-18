import typing as T
from abc import ABC, abstractmethod
from pathlib import Path

import duckdb
import polars
import pyarrow as pa

from data_toolset.utils.mixins import UtilMixin


class BaseUtils(ABC, UtilMixin):
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
    def stats(cls, file_path: Path) -> T.Tuple[int, T.Dict]:
        ...

    @classmethod
    @abstractmethod
    def tail(cls, file_path: Path, *, n: int = 20) -> T.List:
        ...

    @classmethod
    @abstractmethod
    def head(cls, file_path: Path, *, n: int = 20) -> list:
        ...

    @classmethod
    @abstractmethod
    def count(cls, file_path: Path) -> int:
        ...

    @classmethod
    @abstractmethod
    def merge(cls, file_paths: T.List[Path], output_path: Path) -> None:
        ...

    @classmethod
    @abstractmethod
    def validate(cls, file_path: Path, schema_path: Path = None) -> None:
        ...

    @classmethod
    @abstractmethod
    def to_json(cls, file_path: Path, output_path: Path, pretty: bool = False) -> None:
        ...

    @classmethod
    @abstractmethod
    def to_csv(cls, file_path: Path, output_path: Path, has_header: bool = True, delimiter: str = ",",
               line_terminator: str = "\n", quote: str = '\"') -> None:
        ...

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
    def query(cls, file_path: Path, query_expression: str, *, chunk_size: int = 1000000) -> polars.DataFrame:
        """
        Query and filter data in an Avro or Parquet file using SQL-like expressions.

        :param file_path: Path to the Avro or Parquet file to query.
        :type file_path: Path
        :param query_expression: SQL-like query expression to filter and select data.
        :type query_expression: str
        :param chunk_size: Size of data chunks to retrieve per query iteration (default is 1,000,000 rows).
        :type chunk_size: int
        :return: Polars DataFrame containing the result of the query.
        :rtype: polars.DataFrame

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
    def random_sample(cls, file_path: Path, output_path: Path, *, n: int = None, fraction: float = None):
        ...
