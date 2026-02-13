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

1. Configure resources in `config.yaml`
2. Run the pipeline:
   ```bash
   snakemake --cluster "sbatch -p {resources.partition} --mem={resources.mem} -t {resources.time} -c {threads}" -j 23
   ```

## Output

The script entrypoint generates the following files on-disk:

- `dbSNP_156.bcf` - Full filtered BCF file
- `dbSNP_156.chr*.bcf` - Per-chromosome BCF files  
- `dbSNP_156.chr*.lookup.parquet` - Per-chromosome RSID lookup tables

They can be access through the package interface.
