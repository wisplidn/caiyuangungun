#!/usr/bin/env python3
"""
One-time Data Structure Migration Script

This script migrates data from the old, versioned structure (with 'ingest_date')
to the new 'landing' + 'archive' structure.

**OLD STRUCTURE:**
.../{data_type}/{partition_key}=.../ingest_date=.../data.parquet

**NEW STRUCTURE:**
- Landing: .../{data_type}/{partition_key}=.../data.parquet  (Latest version)
- Archive: .../archive/.../{data_type}/{partition_key}=.../data_{timestamp}.parquet (Old versions)

**HOW TO RUN:**
python migrate_data_structure.py --data-type income
"""

import argparse
import json
import shutil
from pathlib import Path
from datetime import datetime

def migrate_data_type(data_type: str, base_path_str: str = "./data"):
    """Migrates a single data type to the new structure."""
    print(f"--- Starting migration for data type: '{data_type}' ---")
    base_path = Path(base_path_str)
    landing_path = base_path / "raw" / "landing" / "tushare" / data_type
    archive_path = base_path / "raw" / "archive" / "tushare" / data_type

    if not landing_path.exists():
        print(f"[SKIP] Landing path for '{data_type}' does not exist. No migration needed.")
        return

    archive_path.mkdir(parents=True, exist_ok=True)
    migrated_partitions = 0

    for partition_dir in landing_path.iterdir():
        if not partition_dir.is_dir():
            continue

        ingest_dirs = sorted([d for d in partition_dir.iterdir() if d.is_dir() and d.name.startswith('ingest_date=')])
        
        if not ingest_dirs:
            # This partition is already in the new format or doesn't have versioned data
            continue

        print(f"  Processing partition: {partition_dir.name}")
        migrated_partitions += 1

        # 1. Identify latest version to move to landing zone
        latest_ingest_dir = ingest_dirs.pop(-1)
        latest_data = latest_ingest_dir / 'data.parquet'
        latest_meta = latest_ingest_dir / 'metadata.json'

        if latest_data.exists():
            shutil.move(str(latest_data), str(partition_dir / 'data.parquet'))
            print(f"    - Moved latest data to {partition_dir}")
        if latest_meta.exists():
            shutil.move(str(latest_meta), str(partition_dir / 'metadata.json'))
            print(f"    - Moved latest metadata to {partition_dir}")

        # 2. Move all older versions to the archive
        archive_partition_path = archive_path / partition_dir.name
        if ingest_dirs:
            archive_partition_path.mkdir(parents=True, exist_ok=True)
            print(f"    - Archiving {len(ingest_dirs)} old version(s)...")

        for old_ingest_dir in ingest_dirs:
            timestamp = datetime.strptime(old_ingest_dir.name.split('=')[1], '%Y-%m-%d').strftime('%Y%m%d_%H%M%S')
            old_data = old_ingest_dir / 'data.parquet'
            old_meta = old_ingest_dir / 'metadata.json'

            if old_data.exists():
                shutil.move(str(old_data), str(archive_partition_path / f'data_{timestamp}.parquet'))
            if old_meta.exists():
                shutil.move(str(old_meta), str(archive_partition_path / f'metadata_{timestamp}.json'))
        
        # 3. Clean up empty ingest directories
        shutil.rmtree(latest_ingest_dir)
        for d in ingest_dirs:
            shutil.rmtree(d)

    print(f"--- Migration complete for '{data_type}'. Migrated {migrated_partitions} partitions. ---")

def main():
    parser = argparse.ArgumentParser(description='Tushare Data Structure Migration Tool.')
    parser.add_argument('--data-type', type=str, required=True,
                        help='The Tushare data type to migrate (e.g., income, balancesheet).')
    parser.add_argument('--base-path', type=str, default='./data',
                        help='The base path for your data.')
    args = parser.parse_args()

    migrate_data_type(args.data_type, args.base_path)

if __name__ == "__main__":
    main()

