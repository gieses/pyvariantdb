#!/usr/bin/env python3
"""Download utilities for dbSNP to Parquet conversion."""

from urllib.request import urlretrieve
from loguru import logger
from pyvariantdb.const import get_cache_dir

def download_dbsnp():
    """
    Download dbSNP VCF file and its index from NCBI's FTP server.

    Downloads:
    - GCF_000001405.40.gz (main VCF file)
    - GCF_000001405.40.gz.tbi (index file)
    """
    logger.info("Starting dbSNP download...")

    urls = [
        "https://ftp.ncbi.nih.gov/snp/latest_release/VCF/GCF_000001405.40.gz",
        "https://ftp.ncbi.nih.gov/snp/latest_release/VCF/GCF_000001405.40.gz.tbi"
    ]
    destination_dir = get_cache_dir()
    logger.info(f"Destination directory: {destination_dir}")

    for url in urls:
        try:
            filename = url.split("/")[-1]
            output_path = destination_dir / filename
            logger.info(f"Downloading {url} to {output_path}...")
            urlretrieve(url, str(output_path))
            logger.info(f"\n✓ Successfully downloaded to {output_path}")
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")
            raise

    logger.info("Download completed successfully!")


if __name__ == "__main__":
    download_dbsnp()
