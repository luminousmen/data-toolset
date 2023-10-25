import logging
from argparse import ArgumentParser, Namespace
from pathlib import Path
import polars

from data_toolset.utils.avro import AvroUtils
from data_toolset.utils.parquet import ParquetUtils

DEFAULT_RECORDS = 20
polars.Config.set_tbl_cols(5000)
polars.Config.set_fmt_str_lengths(5000)


def get_file_format(file_path: Path) -> str:
    """
    Identify file format based on file extension.

    :param file_path: Path to the file whose format needs to be identified.
    :type file_path: Path
    :return: The identified file format (e.g., "avro", "parquet", "csv", "json").
    :rtype: str

    This function takes a file path as input and determines the file format based on its extension.
    It supports several common file formats, including Avro, Parquet, CSV, and JSON.

    :raises ValueError: If the file format is not supported (i.e., if the file extension is unknown).
    """
    ext = file_path.suffix.lower()
    if ext == ".avro":
        return "avro"
    elif ext == ".parquet":
        return "parquet"
    elif ext == ".csv":
        return "csv"
    elif ext == ".json":
        return "json"
    else:
        raise ValueError("Unsupported file format.")


def init_args() -> Namespace:
    """
    Initialize and parse command-line arguments using argparse.

    :return: A namespace containing the parsed command-line arguments.
    :rtype: Namespace
    """
    parser = ArgumentParser()

    subparsers = parser.add_subparsers(help="commands", dest="command", required=True)

    # data-toolset head
    head_parser = subparsers.add_parser("head", help="Print the first N records from a file")
    head_parser.add_argument("file_path", action="store", help="Path to a file")
    head_parser.add_argument("-n", type=int, action="store", default=DEFAULT_RECORDS,
                             help=f"Print count lines of each of the specified files (default is {DEFAULT_RECORDS})")

    # data-toolset tail
    tail_parser = subparsers.add_parser("tail", help="Print the last N records from a file")
    tail_parser.add_argument("file_path", type=Path, action="store", help="Path to a file")
    tail_parser.add_argument("-n", type=int, action="store", default=DEFAULT_RECORDS,
                             help=f"Print count lines of each of the specified files (default is {DEFAULT_RECORDS})")

    # data-toolset meta
    meta_parser = subparsers.add_parser("meta", help="Print a file's metadata")
    meta_parser.add_argument("file_path", type=Path, action="store", help="Path to a file")

    # data-toolset schema
    schema_parser = subparsers.add_parser("schema", help="Print the Avro schema for a file")
    schema_parser.add_argument("file_path", type=Path, action="store", help="Path to a file")

    # data-toolset stats
    stats_parser = subparsers.add_parser("stats", help="Print statistics about a file")
    stats_parser.add_argument("file_path", type=Path, action="store", help="Path to a file")

    # data-toolset query
    query_parser = subparsers.add_parser("query", help="Query a file")
    query_parser.add_argument("file_path", type=Path, action="store", help="Path to a file")
    query_parser.add_argument("query_expression", type=str, action="store", help="Query expression to apply")

    # data-toolset validate
    validate_parser = subparsers.add_parser("validate", help="Validate a file")
    validate_parser.add_argument("file_path", type=Path, action="store", help="Path to a file")
    validate_parser.add_argument("--schema_path", type=Path, action="store", help="Path to the schema file")

    # data-toolset merge
    merge_parser = subparsers.add_parser("merge", help="Merge multiple files into one")
    merge_parser.add_argument("file_path", nargs='+', type=Path, action="store", help="Paths to a files to be merged")
    merge_parser.add_argument("output_path", type=Path, action="store", help="Path to the merged output file")

    # data-toolset count
    count_parser = subparsers.add_parser("count", help="Count the number of records in a file")
    count_parser.add_argument("file_path", type=Path, action="store", help="Path to a file")

    # data-toolset to_json
    to_json_parser = subparsers.add_parser("to_json", help="Convert a file to JSON format")
    to_json_parser.add_argument("file_path", type=Path, action="store", help="Path to the file to convert")
    to_json_parser.add_argument("output_path", type=Path, action="store", help="Path to the output JSON file")
    to_json_parser.add_argument("--pretty", default=False, type=bool, action="store",
                                help="Pretty-print the JSON output (default is False)")

    # data-toolset to_csv
    to_csv_parser = subparsers.add_parser("to_csv", help="Convert a file to CSV format")
    to_csv_parser.add_argument("file_path", type=Path, action="store", help="Path to the file to convert")
    to_csv_parser.add_argument("output_path", type=Path, action="store", help="Path to the output CSV file")
    to_csv_parser.add_argument("--has_header", default=True, type=bool, action="store",
                               help="Specify if the CSV file has a header row (default is True)")
    to_csv_parser.add_argument("--delimiter", default=",", type=str, action="store",
                               help="Specify the delimiter character used in the CSV file (default is ',')")
    to_csv_parser.add_argument("--line_terminator", default="\n", type=str, action="store",
                               help="Specify the line terminator character for the CSV file (default is '\\n')")
    to_csv_parser.add_argument("--quote", default="\"", type=str, action="store",
                               help="Specify the quote character used in the CSV file (default is '\"')")

    # data-toolset to_avro
    to_avro_parser = subparsers.add_parser("to_avro", help="Convert a file to Avro format")
    to_avro_parser.add_argument("file_path", type=Path, action="store", help="Path to the file to convert")
    to_avro_parser.add_argument("output_path", type=Path, action="store", help="Path to the output Avro file")
    to_avro_parser.add_argument("--compression", choices=["uncompressed", "snappy", "deflate"], default="uncompressed",
                                action="store",
                                help="Specify the compression method for the output file (default is 'uncompressed')")

    # data-toolset to_parquet
    to_parquet_parser = subparsers.add_parser("to_parquet", help="Convert a file to Parquet format")
    to_parquet_parser.add_argument("file_path", type=Path, action="store", help="Path to the file to convert")
    to_parquet_parser.add_argument("output_path", type=Path, action="store", help="Path to the output Parquet file")
    to_parquet_parser.add_argument("--compression",
                                   choices=["lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd"],
                                   default="uncompressed", action="store",
                                   help="Specify the compression method for the output file (default is 'uncompressed')")

    # data-toolset random_sample
    random_sample_parser = subparsers.add_parser("random_sample", help="Randomly sample records from a file")
    random_sample_parser.add_argument("file_path", type=Path, action="store", help="Path to the file to sample from")
    random_sample_parser.add_argument("output_path", type=Path, action="store",
                                      help="Path to the output file for sampled records")
    random_sample_parser.add_argument("--n", type=int, default=None, action="store", help="Number of records to sample")
    random_sample_parser.add_argument("--fraction", type=float, default=None, action="store",
                                      help="Fraction of records to sample (0.0 to 1.0)")

    args = parser.parse_args()
    return args


def main() -> None:
    args = init_args()
    # @TODO: need to find a better way for the merge case
    if isinstance(args.file_path, list):
        file_path = Path(args.file_path[0])
    else:
        file_path = Path(args.file_path)
    file_format = get_file_format(file_path)
    if file_format == "avro":
        utils_cls = AvroUtils
    elif file_format == "parquet":
        utils_cls = ParquetUtils
    else:
        raise ValueError("Unsupported file format.")

    if hasattr(utils_cls, args.command):
        function = getattr(utils_cls, args.command)
        function_args = []
        for arg_name in vars(args):
            if arg_name != "command":
                function_args.append(getattr(args, arg_name))
        function(*function_args)
    else:
        raise ValueError("Invalid command.")


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(message)s")
    main()
