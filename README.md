<div align="center">
<img src="https://raw.githubusercontent.com/luminousmen/data-toolset/master/branding/logo/logo.png" width="200">
</div>

[![Master](https://github.com/luminousmen/data-toolset/actions/workflows/master.yml/badge.svg?branch=master)](https://github.com/luminousmen/data-toolset/actions/workflows/master.yml)
[![codecov](https://codecov.io/gh/luminousmen/data-toolset/branch/master/graph/badge.svg?token=6V9IPSRCB0)](https://codecov.io/gh/luminousmen/data-toolset)

# data-tools(et)

data-toolset is designed to simplify your data processing tasks by providing a more user-friendly alternative to the traditional JAR utilities like avro-tools and parquet-tools. With this Python package, you can effortlessly handle various data file formats, including Avro and Parquet, using a simple and intuitive command-line interface.

## Installation

Python 3.8, Python 3.9 and 3.10 are supported and tested (to some extent).

```bash
python -m pip install --user data-toolset
```

## Usage

```bash
$ data-toolset -h
usage: data-toolset [-h] {head,tail,meta,schema,stats,query,validate,merge,count,to_json,to_csv,to_avro,to_parquet,random_sample} ...

positional arguments:
  {head,tail,meta,schema,stats,query,validate,merge,count,to_json,to_csv,to_avro,to_parquet,random_sample}
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
    to_avro             Convert a file to Avro format
    to_parquet          Convert a file to Parquet format
    random_sample       Randomly sample records from a file
```

## Examples

Print the first 10 records of a Parquet file:

```bash
$ data-toolset head my_data.parquet -n 10
shape: (1, 7)
┌───────────┬─────┬──────────┬────────┬──────────────────────────┬────────────────────────────┬──────────────────┐
│ character ┆ age ┆ is_human ┆ height ┆ quote                    ┆ friends                    ┆ appearance       │
│ ---       ┆ --- ┆ ---      ┆ ---    ┆ ---                      ┆ ---                        ┆ ---              │
│ str       ┆ i64 ┆ bool     ┆ f64    ┆ str                      ┆ list[str]                  ┆ struct[2]        │
╞═══════════╪═════╪══════════╪════════╪══════════════════════════╪════════════════════════════╪══════════════════╡
│ Alice     ┆ 10  ┆ true     ┆ 150.5  ┆ Curiouser and curiouser! ┆ ["Rabbit", "Cheshire Cat"] ┆ {"blue","small"} │
└───────────┴─────┴──────────┴────────┴──────────────────────────┴────────────────────────────┴──────────────────┘
```

Query a Parquet file using a SQL-like expression:

```bash
$ data-toolset query my_data.parquet "SELECT * FROM 'my_data.parquet' WHERE height > 165"
shape: (2, 7)
┌─────────────────┬─────┬──────────┬────────┬───────────────────────┬────────────────────────────────────┬───────────────────┐
│ character       ┆ age ┆ is_human ┆ height ┆ quote                 ┆ friends                            ┆ appearance        │
│ ---             ┆ --- ┆ ---      ┆ ---    ┆ ---                   ┆ ---                                ┆ ---               │
│ str             ┆ i64 ┆ bool     ┆ f64    ┆ str                   ┆ list[str]                          ┆ struct[2]         │
╞═════════════════╪═════╪══════════╪════════╪═══════════════════════╪════════════════════════════════════╪═══════════════════╡
│ Mad Hatter      ┆ 35  ┆ true     ┆ 175.2  ┆ I'm late!             ┆ ["Alice"]                          ┆ {"green","tall"}  │
│ Queen of Hearts ┆ 50  ┆ false    ┆ 165.8  ┆ Off with their heads! ┆ ["White Rabbit", "King of Hearts"] ┆ {"red","average"} │
└─────────────────┴─────┴──────────┴────────┴───────────────────────┴────────────────────────────────────┴───────────────────┘
```

Merge multiple Avro files into one:

```bash
data-toolset merge file1.avro file2.avro file3.avro merged_file.avro
```

Convert Avro file into Parquet:

```bash
data-toolset to_parquet my_data.avro output.parquet
```

Convert Parquet file into JSON:

```bash
data-toolset to_json my_data.parquet output.json
```

## Contributing

Contributions are welcome! If you have any suggestions, bug reports, or feature requests, please open an issue on GitHub.

# TODO

- optimizations [TBD]
- benchmarking