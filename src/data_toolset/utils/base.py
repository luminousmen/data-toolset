import typing as T
from abc import ABC, abstractmethod
from pathlib import Path

import duckdb
import pyarrow as pa

from data_toolset.utils.mixins import UtilMixin


class BaseUtils(ABC, UtilMixin):
    @classmethod
    @abstractmethod
    def to_arrow_table(cls, file_path: Path) -> pa.Table:
        ...

    @classmethod
    @abstractmethod
    def write_arrow_table(cls, table: pa.Table, file_path: Path) -> None:
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
    def tail(cls, file_path: Path, n: int = 20) -> T.List:
        ...

    @classmethod
    @abstractmethod
    def head(cls, file_path: Path, n: int = 20) -> list:
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
    def to_json(cls, file_path: Path, output_path: Path) -> None:
        ...

    @classmethod
    @abstractmethod
    def to_csv(cls, file_path: Path, output_path: Path, delimiter=',') -> None:
        ...

    @classmethod
    def query(cls, file_path: Path, query_expression: str, chunk_size: int = 1000000) -> pa.Table:
        """
        Query and filter data in an Avro or Parquet file using SQL-like expressions.

        :param file_path: Path to the Avro or Parquet file to query.
        :type file_path: Path
        :param query_expression: SQL-like query expression to filter and select data.
        :type query_expression: str
        :param chunk_size: Size of data chunks to retrieve per query iteration (default is 1,000,000 rows).
        :type chunk_size: int
        :return: Arrow Table containing the result of the query.
        :rtype: pa.Table

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
        data = pa.Table.from_batches(batches=all_chunks)
        cls._print_table(data)
        return data
