#!/usr/bin/env python3
"""
Main entry point for the scraper framework.
This script demonstrates using various scraper plugins.
"""

import argparse
import logging
import sys
from pathlib import Path

from scraper.config.settings import OUTPUT_DIR
from scraper.utils.logging import configure_file_logging
from scraper.plugins.gst import main as gst_main


def setup_logging():
    """Configure logging for the application."""
    log_file = OUTPUT_DIR / "scraper.log"
    configure_file_logging(log_file)
    logging.info("Logging configured.")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Web scraper framework")
    parser.add_argument("--plugin", choices=["gst"], default="gst",
                        help="Scraper plugin to use")
    parser.add_argument("--input", required=False,
                        help="Path to input file (CSV or Excel)")
    parser.add_argument("--output", required=False,
                        help="Path to output file (CSV or Excel)")
    parser.add_argument("--format", choices=["csv", "excel"], default=None,
                        help="Force output format regardless of file extension")

    return parser.parse_args()


def main():
    """Main entry point for the application."""
    setup_logging()
    args = parse_args()

    try:
        if args.plugin == "gst":
            # Default input and output files
            input_file = args.input or "data/gst_details.csv"
            output_file = args.output or "data/output/gst_dump.csv"

            # Override output extension if format is specified
            if args.format:
                output_path = Path(output_file)
                if args.format == "excel" and output_path.suffix.lower() not in ['.xlsx', '.xls']:
                    output_file = str(output_path.with_suffix('.xlsx'))
                elif args.format == "csv" and output_path.suffix.lower() != '.csv':
                    output_file = str(output_path.with_suffix('.csv'))

            logging.info(f"Using input file: {input_file}")
            logging.info(f"Using output file: {output_file}")

            # Run the GST scraper
            gst_main(input_file=input_file, output_file=output_file)
        # Add more plugins here
        else:
            print(f"Plugin {args.plugin} not found")
            sys.exit(1)

    except Exception as e:
        logging.error(f"Error running {args.plugin} plugin: {e}")
        sys.exit(1)

    logging.info("Scraping completed successfully")
    print("Scraping completed successfully")


if __name__ == "__main__":
    main()
