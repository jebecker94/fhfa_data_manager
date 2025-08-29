# -*- coding: utf-8 -*-
"""
Smoke test: download FHFA zips and FHLB CSVs into configured raw directories.

Usage (PowerShell):
  python scripts/download_smoke.py --url https://www.fhfa.gov/data/pudb

Relies on environment variables (or defaults) resolved by PathsConfig.
"""

from __future__ import annotations
import argparse
from pathlib import Path

from data_loaders import PathsConfig, ImportOptions, FHFADataLoader, FHLBDataLoader


def main() -> None:
    parser = argparse.ArgumentParser(description='Download FHFA and FHLB datasets for smoke testing.')
    parser.add_argument('--url', required=True, help='Base FHFA PUDB page URL to scrape for links')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite any existing downloaded files')
    parser.add_argument('--pause', type=int, default=None, help='Pause seconds between downloads (default from ImportOptions)')
    args = parser.parse_args()

    paths = PathsConfig.from_env()
    options = ImportOptions(overwrite=args.overwrite)

    print(f'Using project directory: {paths.project_dir}')
    print(f'Raw dir: {paths.raw_dir}')

    fhfa = FHFADataLoader(paths, options)
    fhlb = FHLBDataLoader(paths, options)

    fhfa_raw_dir = paths.raw_dir / 'fhfa'
    fhlb_raw_dir = paths.raw_dir / 'fhlb'

    print('Downloading FHFA zip files...')
    fhfa.download(args.url, fhfa_raw_dir, include_substrings=('pudb',), pause_seconds=args.pause)

    print('Downloading FHFA dictionary PDFs...')
    dict_dir = Path(paths.project_dir) / 'dictionary_files'
    fhfa.download_dictionaries(args.url, dict_dir, pause_seconds=args.pause)

    print('Downloading FHLB CSV files...')
    fhlb.download(args.url, fhlb_raw_dir, include_substrings=('pudb',), pause_seconds=args.pause)

    print('Download smoke test complete.')


if __name__ == '__main__':
    main()


