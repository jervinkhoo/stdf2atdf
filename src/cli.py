# src/cli.py
from pathlib import Path
import argparse
import logging

from .core.utils.files import find_stdf_files
from .core.utils.services import process_files

from .core.utils.logging import setup_logging

logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(description='STDF to ATDF conversion tool')
    # Existing arguments
    parser.add_argument('input',
                        help='Input STDF file or directory containing STDF files')
    parser.add_argument('--output', '-o',
                        action='store_true',
                        help='Generate ATDF output files (using input filename with .atdf extension)')
    parser.add_argument('--database', '-d',
                        action='store_true',
                        help='Generate SQLite database files (using input filename with .db extension)')
    parser.add_argument('--records', '-r',
                        nargs='*',
                        help='Specific record types to process')
    parser.add_argument('--workers', '-w',
                        type=int,
                        default=None,
                        help='Number of parallel workers (defaults to optimal based on system resources)')

    # Simplified preprocessor argument
    parser.add_argument('--preprocessor', '-p',
                        choices=['advantest', 'teradyne', 'eagle'],
                        help='Specify the preprocessor to use')

    return parser.parse_args()


def main():
    args = parse_arguments()
    input_path = Path(args.input)

    try:
        input_files = find_stdf_files(input_path)

        if not input_files:
            logger.error(f"No STDF files found in {input_path}")
            return

        logger.info(f"Found {len(input_files)} STDF files to process")

        # Process all files
        process_files(
            input_files,
            output=args.output,
            database=args.database,
            records=args.records,
            max_workers=args.workers,
            preprocessor_type=args.preprocessor
        )

        logger.info("Conversion completed successfully")

    except Exception as e:
        logger.error(f"Conversion failed: {str(e)}")
        raise


if __name__ == "__main__":
    setup_logging()
    main()