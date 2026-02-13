#!/usr/bin/env python3
"""Run the dbSNP to Parquet Snakemake pipeline.

This pipeline will perform the steps:

1. Access already downloaded dbSNP file (`pyvariantdb-download` result)
2. Filters for SNVs only
3. Converts chromosome contigs to standard naming
4. Splits data by chromosome
5. Creates Parquet lookup tables with RSID mappings

Example usage:
    pyvariantdb-make-dbsnp -j 10 -c 10
"""
import argparse
import subprocess
import sys
from importlib.resources import files
from loguru import logger


def main():
    """Run Snakemake pipeline with specified parameters."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "-j", "--jobs", type=int, default=8, help="Number of jobs (default: 8)"
    )
    parser.add_argument(
        "-c",
        "--cores",
        type=int,
        default=20,
        help="Number of cores (alternative to --jobs)",
    )
    parser.add_argument("--config", type=str, help="Path to config YAML file")

    args = parser.parse_args()

    # Get Snakefile path
    snakefile = files("pyvariantdb") / "assets" / "Snakefile"

    # Build command
    cmd = ["snakemake", "-s", str(snakefile), "--rerun-incomplete"]

    if args.config:
        cmd.extend(["--configfile", args.config])

    # parallelism
    cmd.extend(["--cores", str(args.cores)])
    cmd.extend(["-j", str(args.jobs)])

    # Run snakemake
    logger.info(f"Running: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)
    except FileNotFoundError:
        print("Error: snakemake not found", file=sys.stderr)
        sys.exit(1)
    logger.success("Completed dbsnp2parquet.")


if __name__ == "__main__":
    main()
