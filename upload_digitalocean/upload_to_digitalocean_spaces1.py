#!/usr/bin/env python3
"""
Upload Transformed Data to DigitalOcean Spaces

Uploads all transformed market data to DigitalOcean Spaces (S3-compatible storage)
in the Bangalore (blr1) region for production use.

Configuration:
- Endpoint: https://blr1.digitaloceanspaces.com
- Space: historical-db-tick
- Region: blr1 (Bangalore)
- Access via S3-compatible API
- Credentials: Built-in (hardcoded)

Data Structure in Space:
    raw/parquet_data/
    ├── cash/
    │   └── [symbol]/YYYY/MM/*.parquet
    ├── options/
    │   └── [symbol_call|symbol_put]/YYYY/MM/*.parquet
    └── futures/
        └── [symbol]/YYYY/MM/*.parquet

Features:
- Parallel multi-part uploads
- Progress tracking
- Resumable transfers
- Organized by category (cash/options/futures)

Usage:
    # Upload from flat input folder (symbol_type_DDMMYYYY.parquet format)
    python3 upload_to_digitalocean_spaces.py --flat-input --source-dir ./input

    # Upload all data from organized structure (credentials are built-in)
    python3 upload_to_digitalocean_spaces.py

    # Specific symbols only
    python3 upload_to_digitalocean_spaces.py --symbols banknifty nifty

    # Dry run (test without uploading)
    python3 upload_to_digitalocean_spaces.py --flat-input --source-dir ./input --dry-run

Author: MarvelQuant
Date: 2025-10-28
"""

import os
import sys
import boto3
import logging
import argparse
from pathlib import Path
from typing import List
from datetime import datetime
from collections import defaultdict
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

# Logging configuration
def setup_logging():
    """Setup logging with cross-platform log file path"""
    # Use current directory for log file (works on both Windows and Linux)
    log_file = Path('./digitalocean_upload.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# DigitalOcean Spaces configuration
DO_ENDPOINT = "https://blr1.digitaloceanspaces.com"
DO_REGION = "blr1"
SPACE_NAME = "historical-db-tick"
SPACE_PREFIX = "raw/parquet_data"


class DigitalOceanSpacesUploader:
    """Upload data to DigitalOcean Spaces"""

    SYMBOLS = ["banknifty", "nifty", "midcapnifty", "sensex", "crudeoil", "naturalgas", "finnifty", "silverm", "goldm"]

    def __init__(
        self,
        source_root: str = "/nvme0n1-disk/parquet_data",
        access_key: str = None,
        secret_key: str = None,
        symbols: List[str] = None,
        max_workers: int = 10,
        dry_run: bool = False,
        flat_input: bool = False
    ):
        self.source_root = Path(source_root)
        self.symbols = symbols or self.SYMBOLS
        self.max_workers = max_workers
        self.dry_run = dry_run
        self.flat_input = flat_input  # If True, expect flat file structure in source_root

        # Hardcoded credentials
        self.access_key = "DO00CDX8Z7BFTQJ9W2AZ"
        self.secret_key = "kR159s1x7Xjfd6RhVIyw8X34UGLIFKSzP7/0fG6Yt9I"

        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            region_name=DO_REGION,
            endpoint_url=DO_ENDPOINT,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )

        # Stats
        self.stats = {
            'total_files': 0,
            'uploaded': 0,
            'skipped': 0,
            'failed': 0,
            'total_bytes': 0,
            'by_category': defaultdict(lambda: defaultdict(lambda: {'files': 0, 'bytes': 0}))
        }

    def verify_space_exists(self) -> bool:
        """Verify that the Space exists"""
        try:
            self.s3_client.head_bucket(Bucket=SPACE_NAME)
            logger.info(f"✅ Space '{SPACE_NAME}' exists and is accessible")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"❌ Space '{SPACE_NAME}' does not exist")
            elif error_code == '403':
                logger.error(f"❌ Access denied to Space '{SPACE_NAME}'")
            else:
                logger.error(f"❌ Error accessing Space: {e}")
            return False

    def discover_files(self) -> List[tuple]:
        """Discover all files to upload with new structure"""
        files_to_upload = []

        logger.info("Discovering files to upload...")

        if self.flat_input:
            # Handle flat file structure (e.g., symbol_type_DDMMYYYY.parquet)
            self._discover_flat_files(files_to_upload)
        else:
            # Handle organized directory structure
            self._discover_structured_files(files_to_upload)

        logger.info(f"[OK] Discovered {len(files_to_upload)} files to upload")

        # Print breakdown by category and symbol
        category_counts = defaultdict(lambda: defaultdict(lambda: {'files': 0, 'bytes': 0}))
        
        for file_path, _, category_symbol in files_to_upload:
            # Extract category from category_symbol tuple
            category, symbol = category_symbol if isinstance(category_symbol, tuple) else ('unknown', category_symbol)
            file_size = file_path.stat().st_size
            category_counts[category][symbol]['files'] += 1
            category_counts[category][symbol]['bytes'] += file_size

        # Print organized summary
        for category in sorted(category_counts.keys()):
            logger.info(f"\n  {category.upper()}:")
            for symbol in sorted(category_counts[category].keys()):
                stats = category_counts[category][symbol]
                size_mb = stats['bytes'] / (1024 * 1024)
                logger.info(f"    {symbol:20s}: {stats['files']:6d} files | {size_mb:8.1f} MB")

        total_files = sum(stats['files'] for cat in category_counts.values() for stats in cat.values())
        total_bytes = sum(stats['bytes'] for cat in category_counts.values() for stats in cat.values())
        total_size_gb = total_bytes / (1024 * 1024 * 1024)
        logger.info(f"\n  {'TOTAL':15s}: {total_files:6d} files | {total_size_gb:8.2f} GB")

        return files_to_upload

    def _discover_flat_files(self, files_to_upload: list):
        """Discover files from flat directory structure"""
        import re
        from datetime import datetime
        
        # Pattern: symbol_type_DDMMYYYY.parquet
        pattern = re.compile(r'^(.+?)_(call|put|future|cash)_(\d{8})\.parquet$', re.IGNORECASE)
        
        if not self.source_root.exists():
            logger.error(f"Source directory not found: {self.source_root}")
            return
        
        for file_path in self.source_root.glob('*.parquet'):
            if not file_path.is_file():
                continue
                
            match = pattern.match(file_path.name)
            if not match:
                logger.warning(f"Skipping file with unexpected format: {file_path.name}")
                continue
            
            symbol = match.group(1).lower()
            file_type = match.group(2).lower()
            date_str = match.group(3)  # DDMMYYYY
            
            # Filter by symbols if specified
            if self.symbols and symbol not in self.symbols:
                continue
            
            # Parse date: DDMMYYYY -> YYYY/MM
            try:
                date_obj = datetime.strptime(date_str, '%d%m%Y')
                year = date_obj.strftime('%Y')
                month = date_obj.strftime('%m')
            except ValueError:
                logger.warning(f"Invalid date format in filename: {file_path.name}")
                continue
            
            # Map file type to category
            if file_type == 'cash':
                category = 'cash'
                s3_symbol = symbol
            elif file_type in ['call', 'put']:
                category = 'options'
                s3_symbol = f"{symbol}_{file_type}"
            elif file_type == 'future':
                category = 'futures'
                s3_symbol = symbol
            else:
                logger.warning(f"Unknown type '{file_type}' in file: {file_path.name}")
                continue
            
            # Build S3 key: raw/parquet_data/category/symbol/YYYY/MM/filename
            s3_key = f"{SPACE_PREFIX}/{category}/{s3_symbol}/{year}/{month}/{file_path.name}"
            
            files_to_upload.append((file_path, s3_key, (category, s3_symbol)))
            logger.debug(f"Added: {file_path.name} -> {s3_key}")

    def _discover_structured_files(self, files_to_upload: list):
        """Discover files from organized directory structure"""
        # Categories to process
        categories = ['cash', 'options', 'futures']

        for symbol in self.symbols:
            # Process each category (cash, options, futures)
            for category in categories:
                category_dir = self.source_root / category / symbol
                
                if not category_dir.exists():
                    # Try alternative paths for options (symbol_call, symbol_put)
                    if category == 'options':
                        for option_type in ['call', 'put']:
                            option_dir = self.source_root / category / f"{symbol}_{option_type}"
                            if option_dir.exists():
                                self._process_directory(option_dir, files_to_upload, category, f"{symbol}_{option_type}")
                    continue
                
                self._process_directory(category_dir, files_to_upload, category, symbol)

    def _process_directory(self, base_dir: Path, files_to_upload: list, category: str, symbol: str):
        """Process files in a directory and add to upload list"""
        # Find all parquet files in YYYY/MM structure
        for file_path in base_dir.rglob('*.parquet'):
            if file_path.is_file():
                # Extract YYYY and MM from the path or filename
                parts = file_path.relative_to(base_dir).parts
                
                # Expected structure: YYYY/MM/filename.parquet
                if len(parts) >= 3:
                    year = parts[0]
                    month = parts[1]
                    filename = parts[2]
                else:
                    # If structure is different, try to extract from filename
                    logger.warning(f"Unexpected structure: {file_path}, skipping")
                    continue
                
                # Build S3 key: raw/parquet_data/category/symbol/YYYY/MM/filename
                s3_key = f"{SPACE_PREFIX}/{category}/{symbol}/{year}/{month}/{filename}"
                
                files_to_upload.append((file_path, s3_key, (category, symbol)))

    def file_exists_in_space(self, s3_key: str, local_path: Path) -> bool:
        """Check if file already exists in Space with same size"""
        try:
            response = self.s3_client.head_object(Bucket=SPACE_NAME, Key=s3_key)
            remote_size = response['ContentLength']
            local_size = local_path.stat().st_size

            return remote_size == local_size
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.warning(f"Error checking {s3_key}: {e}")
                return False

    def upload_file(self, local_path: Path, s3_key: str, category_symbol) -> dict:
        """Upload a single file to Spaces"""
        # Handle both tuple (category, symbol) and legacy string format
        if isinstance(category_symbol, tuple):
            category, symbol = category_symbol
        else:
            category, symbol = 'unknown', category_symbol
            
        result = {
            'file': str(local_path),
            's3_key': s3_key,
            'category': category,
            'symbol': symbol,
            'status': 'pending',
            'size': 0,
            'error': None
        }

        try:
            file_size = local_path.stat().st_size
            result['size'] = file_size

            # Check if already exists (skip if same size)
            if self.file_exists_in_space(s3_key, local_path):
                result['status'] = 'skipped'
                logger.debug(f"⏭️  Skipped (exists): {s3_key}")
                return result

            if self.dry_run:
                result['status'] = 'dry_run'
                logger.info(f"🔍 DRY RUN: Would upload {s3_key} ({file_size / 1024 / 1024:.2f} MB)")
                return result

            # Upload file
            self.s3_client.upload_file(
                str(local_path),
                SPACE_NAME,
                s3_key,
                ExtraArgs={'ACL': 'private'}  # Private by default
            )

            result['status'] = 'uploaded'
            logger.info(f"✅ Uploaded: {s3_key} ({file_size / 1024 / 1024:.2f} MB)")
            return result

        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            logger.error(f"❌ Failed: {s3_key}: {e}")
            return result

    def run(self):
        """Execute upload pipeline"""
        logger.info("=" * 100)
        logger.info("DIGITALOCEAN SPACES UPLOAD")
        logger.info("=" * 100)
        logger.info(f"Source root: {self.source_root}")
        logger.info(f"Input mode: {'Flat file structure' if self.flat_input else 'Organized directory structure'}")
        logger.info(f"Endpoint: {DO_ENDPOINT}")
        logger.info(f"Space: {SPACE_NAME}")
        logger.info(f"Region: {DO_REGION}")
        logger.info(f"Prefix: {SPACE_PREFIX}")
        logger.info(f"Symbols: {', '.join(self.symbols)}")
        logger.info(f"Max workers: {self.max_workers}")
        logger.info(f"Dry run: {self.dry_run}")
        logger.info("=" * 100)

        # Verify Space exists
        if not self.dry_run and not self.verify_space_exists():
            logger.error("Cannot proceed without accessible Space")
            return

        # Discover files
        files_to_upload = self.discover_files()

        if not files_to_upload:
            logger.error("No files to upload!")
            return

        self.stats['total_files'] = len(files_to_upload)

        # Upload files
        logger.info(f"\n⏱️  Starting upload at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Uploading {len(files_to_upload)} files with {self.max_workers} workers...\n")

        start_time = datetime.now()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self.upload_file, local_path, s3_key, symbol): (local_path, s3_key, symbol)
                for local_path, s3_key, symbol in files_to_upload
            }

            for future in as_completed(future_to_file):
                local_path, s3_key, category_symbol = future_to_file[future]

                try:
                    result = future.result()
                    
                    category = result.get('category', 'unknown')
                    symbol = result.get('symbol', 'unknown')

                    # Update stats
                    if result['status'] == 'uploaded':
                        self.stats['uploaded'] += 1
                        self.stats['total_bytes'] += result['size']
                        self.stats['by_category'][category][symbol]['files'] += 1
                        self.stats['by_category'][category][symbol]['bytes'] += result['size']
                    elif result['status'] == 'skipped':
                        self.stats['skipped'] += 1
                    elif result['status'] == 'failed':
                        self.stats['failed'] += 1
                    elif result['status'] == 'dry_run':
                        self.stats['uploaded'] += 1  # Count as uploaded for dry run

                    # Progress update every 100 files
                    processed = self.stats['uploaded'] + self.stats['skipped'] + self.stats['failed']
                    if processed % 100 == 0:
                        progress_pct = processed / len(files_to_upload) * 100
                        logger.info(f"📈 Progress: {processed}/{len(files_to_upload)} "
                                  f"({progress_pct:.1f}%) | ✅ {self.stats['uploaded']} | "
                                  f"⏭️  {self.stats['skipped']} | ❌ {self.stats['failed']}")

                except Exception as e:
                    logger.error(f"Error processing {local_path}: {e}")
                    self.stats['failed'] += 1

        # Print summary
        end_time = datetime.now()
        duration = end_time - start_time

        self.print_summary(duration)

    def print_summary(self, duration):
        """Print upload summary"""
        logger.info("\n" + "=" * 100)
        logger.info("UPLOAD COMPLETE" if not self.dry_run else "DRY RUN COMPLETE")
        logger.info("=" * 100)
        logger.info(f"Duration: {duration}")

        logger.info(f"\n📊 Overall Statistics:")
        logger.info(f"  Total files:       {self.stats['total_files']:,}")
        logger.info(f"  ✅ Uploaded:        {self.stats['uploaded']:,}")
        logger.info(f"  ⏭️  Skipped:         {self.stats['skipped']:,}")
        logger.info(f"  ❌ Failed:          {self.stats['failed']:,}")
        logger.info(f"  Total size:        {self.stats['total_bytes'] / (1024**3):.2f} GB")

        if self.stats['uploaded'] > 0 and duration.total_seconds() > 0:
            avg_speed = self.stats['total_bytes'] / duration.total_seconds() / (1024 * 1024)
            logger.info(f"  Average speed:     {avg_speed:.2f} MB/s")

        logger.info(f"\n📊 Per-Category Statistics:")
        for category in sorted(self.stats['by_category'].keys()):
            logger.info(f"\n  {category.upper()}:")
            for symbol in sorted(self.stats['by_category'][category].keys()):
                stats = self.stats['by_category'][category][symbol]
                size_mb = stats['bytes'] / (1024 * 1024)
                logger.info(f"    {symbol:20s}: {stats['files']:6d} files | {size_mb:8.1f} MB")

        logger.info(f"\n🌐 Access URL:")
        logger.info(f"  Space Endpoint: {DO_ENDPOINT}")
        logger.info(f"  Space Name: {SPACE_NAME}")
        logger.info(f"  Data Path: {SPACE_PREFIX}/")
        logger.info(f"\n📁 Structure:")
        logger.info(f"  {SPACE_PREFIX}/cash/[symbol]/YYYY/MM/*.parquet")
        logger.info(f"  {SPACE_PREFIX}/options/[symbol_call|symbol_put]/YYYY/MM/*.parquet")
        logger.info(f"  {SPACE_PREFIX}/futures/[symbol]/YYYY/MM/*.parquet")

        logger.info("=" * 100)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Upload data to DigitalOcean Spaces")
    parser.add_argument(
        '--symbols',
        nargs='+',
        choices=['banknifty', 'nifty', 'midcapnifty', 'sensex', 'crudeoil', 'naturalgas', 'finnifty', 'silverm', 'goldm'],
        help='Specific symbols to upload (default: all)'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=10,
        help='Maximum parallel workers (default: 10)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test without actually uploading'
    )
    parser.add_argument(
        '--flat-input',
        action='store_true',
        help='Use flat file structure (symbol_type_DDMMYYYY.parquet)'
    )
    parser.add_argument(
        '--source-dir',
        type=str,
        default=None,
        help='Source directory path (default: /nvme0n1-disk/parquet_data or specify input folder)'
    )

    args = parser.parse_args()

    # Determine source directory
    source_dir = args.source_dir
    if not source_dir:
        if args.flat_input:
            source_dir = "./input"  # Default for flat input
        else:
            source_dir = "/nvme0n1-disk/parquet_data"  # Default for structured

    # Create uploader
    try:
        uploader = DigitalOceanSpacesUploader(
            source_root=source_dir,
            symbols=args.symbols,
            max_workers=args.max_workers,
            dry_run=args.dry_run,
            flat_input=args.flat_input
        )

        # Run
        uploader.run()

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Upload interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Upload failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
