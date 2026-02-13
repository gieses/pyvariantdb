"""
Convert BCF/VCF files to Parquet format.

This module reads genetic variant data from BCF/VCF files using cyvcf2
and writes the processed variants to Parquet format for efficient storage
and analysis.
"""

import argparse
import gc
import sys
from pathlib import Path
from typing import Optional

import cyvcf2 as cy
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

# Configuration constants
DEFAULT_BATCH_SIZE = 500_000
SCHEMA = pa.schema([
    pa.field("RSID", pa.string()),
    pa.field("ID", pa.string())
])


def setup_logging(debug: bool = False) -> None:
    """
    Configure loguru logging.

    Args:
        debug: Enable debug-level logging if True.
    """
    log_level = "DEBUG" if debug else "INFO"
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        format="<level>{time:YYYY-MM-DD HH:mm:ss}</level> | <level>{level: <8}</level> | <level>{message}</level>",
        level=log_level
    )


def validate_input_file(input_path: str) -> Path:
    """
    Validate that the input BCF/VCF file exists.

    Args:
        input_path: Path to the input BCF/VCF file.

    Returns:
        Path: Pathlib Path object of the input file.

    Raises:
        FileNotFoundError: If the input file doesn't exist.
    """
    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    logger.info(f"Input file: {input_file}")
    return input_file


def process_variant(variant) -> Optional[dict]:
    """
    Process a single variant from the VCF/BCF file.

    Filters out variants with no ALT alleles and creates a row dictionary
    with RSID and ID fields.

    Args:
        variant: A cyvcf2 Variant object.

    Returns:
        dict: A dictionary with RSID and ID fields, or None if variant is filtered out.
    """
    # Skip variants with no ALT alleles
    if len(variant.ALT) < 1:
        logger.warning(
            f"Skipping variant {variant.ID}: no ALT alleles "
            f"(variant.ALT={variant.ALT}, variant.end={variant.end})"
        )
        return None

    return {
        "RSID": variant.ID,
        "ID": f"{variant.CHROM}_{variant.end}_{variant.REF}_{variant.ALT[0]}",
    }


def convert_vcf_to_parquet(
    input_file: Path,
    output_file: Path,
    batch_size: int = DEFAULT_BATCH_SIZE
) -> None:
    """
    Convert a BCF/VCF file to Parquet format.

    Reads variants from a BCF/VCF file, processes them in batches,
    and writes the results to a Parquet file.

    Args:
        input_file: Path to the input BCF/VCF file.
        output_file: Path to the output Parquet file.
        batch_size: Number of variants to process before writing a batch.
                   Default is 500000.
    """
    logger.info(f"Starting conversion of {input_file} to {output_file}")
    logger.info(f"Batch size: {batch_size:,}")

    writer = pq.ParquetWriter(str(output_file), SCHEMA)

    try:
        variant_count = 0
        df = None

        with cy.VCF(str(input_file)) as vcf:
            for variant in vcf:
                # Process the variant
                row_data = process_variant(variant)
                if row_data is None:
                    continue

                # Convert row to Polars DataFrame
                import polars as pl
                row_df = pl.DataFrame(row_data)

                # Initialize or append to batch dataframe
                if df is None:
                    df = row_df.clone()
                else:
                    df.vstack(row_df, in_place=True)

                variant_count += 1

                # Write batch when batch size is reached
                if variant_count % batch_size == 0 and variant_count > 0:
                    batch = pa.RecordBatch.from_pandas(df.to_pandas())
                    writer.write_batch(batch)
                    logger.info(
                        f"Processed {variant_count:,} variants | "
                        f"Batch written to {output_file}"
                    )
                    df = None  # Reset for next batch

                # Log progress every 100k variants
                if variant_count % 100000 == 0:
                    logger.info(f"Progress: {variant_count:,} variants processed")
                    gc.collect()

        # Write remaining variants if any
        if df is not None:
            batch = pa.RecordBatch.from_pandas(df.to_pandas())
            writer.write_batch(batch)
            logger.info(
                f"Final batch written to {output_file} "
                f"({variant_count:,} total variants)"
            )

        writer.close()
        logger.info(
            f"✓ Conversion completed successfully\n"
            f"  Total variants processed: {variant_count:,}\n"
            f"  Output file: {output_file}"
        )

    except Exception as e:
        logger.error(f"Error during conversion: {e}")
        writer.close()
        raise


def main() -> int:
    """
    Main entry point for the conversion script.

    Returns:
        int: Exit code (0 for success, 1 for failure).
    """
    parser = argparse.ArgumentParser(
        description="Convert BCF/VCF files to Parquet format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python convert_to_parquet.py input.bcf output.parquet
  python convert_to_parquet.py -b 1000000 input.vcf.gz output.parquet
  python convert_to_parquet.py --debug input.bcf output.parquet
        """
    )

    parser.add_argument(
        "input",
        type=str,
        help="Path to input BCF/VCF file"
    )
    parser.add_argument(
        "output",
        type=str,
        help="Path to output Parquet file"
    )
    parser.add_argument(
        "-b", "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Batch size for writing (default: {DEFAULT_BATCH_SIZE:,})"
    )
    parser.add_argument(
        "-d", "--debug",
        action="store_true",
        help="Enable debug logging"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(debug=args.debug)

    try:
        # Validate input file
        input_file = validate_input_file(args.input)
        output_file = Path(args.output)

        # Create output directory if needed
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Perform conversion
        convert_vcf_to_parquet(
            input_file=input_file,
            output_file=output_file,
            batch_size=args.batch_size
        )

        return 0

    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if args.debug:
            logger.exception("Full traceback:")
        return 1


if __name__ == "__main__":
    sys.exit(main())
