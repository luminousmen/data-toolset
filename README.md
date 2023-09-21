[![Master](https://github.com/luminousmen/data-toolset/actions/workflows/master.yml/badge.svg?branch=master)](https://github.com/luminousmen/data-toolset/actions/workflows/master.yml)
[![codecov](https://codecov.io/gh/luminousmen/data-toolset/branch/master/graph/badge.svg?token=6V9IPSRCB0)](https://codecov.io/gh/luminousmen/data-toolset)

# data-tools(et)

data-toolset is designed to simplify your data processing tasks by providing a more user-friendly alternative to the traditional JAR utilities like avro-tools and parquet-tools. With this Python package, you can effortlessly handle various data file formats, including Avro and Parquet, using a simple and intuitive command-line interface.

## Installation

Python 3.9 and 3.10 are supported and tested (to some extent).

```bash
python -m pip install --user data-toolset
```

## Usage

```bash
$ data-toolset -h
usage: data-toolset [-h] {head,tail,meta,schema,stats,query,validate,merge,count,to_json,to_csv} ...

positional arguments:
  {head,tail,meta,schema,stats,query,validate,merge,count,to_json,to_csv}
                        commands
    head                Print the first N records from a file
    tail                Print the last N records from a file
    meta                Print a file's metadata
    schema              Print the Avro schema for a file
    stats               Print statistics about a file
    query               Query a file
    validate            Validate a file
    merge               Merge multiple files into one
    count               Count the number of records in a file
    to_json             Convert a file to JSON format
    to_csv              Convert a file to CSV format

optional arguments:
  -h, --help            show this help message and exit
```

## Examples

Print the first 10 records of a Parquet file:

```bash
data-toolset head my_data.parquet -n 10
```

Query a Parquet file using a SQL-like expression:

```bash
data-toolset query my_data.parquet "SELECT * FROM 'my_data.parquet' WHERE age > 25"
```

Merge multiple Avro files into one:

```bash
data-toolset merge file1.avro file2.avro file3.avro merged_file.avro
```

## Contributing

Contributions are welcome! If you have any suggestions, bug reports, or feature requests, please open an issue on GitHub.

# TODO

- make parquet validation work with avsc schemas?
- create random_sample function
- create schema_evolution function
- mature create_sample function
- optimizations [TBD]
- support 3.11+
