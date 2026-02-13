# dbSNP to Parquet Converter

A python-package focused rewrite of [Weinstock's dbSNP](https://github.com/weinstockj/dbSNP_to_parquet) to parquet repository.
This package allows convenient download, processing and access to data from dbSNP. A simple python interface
can be used to work with the data once it is processed.

## Installation

`pyvariantdb` is available on PyPI and can be installed from there with the package management tool of choice. For
development we use pixi:

```bash
# install pixi
curl -fsSL https://pixi.sh/install.sh | sh
```

## Usage

We recommend to prepare the data from the command line before using the package since download and processing takes
quit some time. Per default the data is stored at ~/.cache/pyvariantdb. This can be changed through the usage of
environment variables:

```bash
export PYVARIANTDB_HOME = "/raid/cache/pyvariantdb"
```

Execution of the pipeline can be done with:

```bash
# default params
pixi run pyvariantdb-download
# snakemake --cluster "sbatch -p {resources.partition} --mem={resources.mem} -t {resources.time} -c {threads}" -j 23
```


## Processing Pipeline

`pyvariantdb` offers some quality of life improvements for working with dbSNP and the original repository.
The original pipeline remains the same:

1. Downloads dbSNP data (GRCh38 build 156)
2. Filters for SNVs only
3. Converts chromosome contigs to standard naming
4. Splits data by chromosome 
5. Creates Parquet lookup tables with RSID mappings

## Usage

We aim to provide an easy interface to extract genomic coordinates from dbSNP. For this, the main functionality
is extremely simple. Just provide the rs-identifier (with or without chromosome) and we fetch the respective
coordindates. However, to achieve this we need to run the pipeline mentioned above.

Extracting coordinates is simple:

```python
    from pyvariantdb.lookup import SNPLookup
    lookup = SNPLookup()
    # P53 mutations, chromosome 17
    rsids = ["rs1042522", "rs17878362", "rs1800372"]
    df_all = lookup.query_all(rsids)
    df_chr = lookup.query_chromosome(rsids, "17")
    print(df_all)
    print(df_chr) 
```

To build the respective databases we need to run the data processing pipeline.

```bash
# optionally, we can pass a config for the underlying snakemake workflow with --config
pyvariantdb-make-dbsnp --jobs 2 --cores 8
```


## Output

The script entrypoint generates the following files on-disk:

- `dbSNP_156.bcf` - Full filtered BCF file
- `dbSNP_156.chr*.bcf` - Per-chromosome BCF files  
- `dbSNP_156.chr*.lookup.parquet` - Per-chromosome RSID lookup tables

They can be access through the package interface.
