#!/usr/bin/env python3
"""
Command-line entrypoint for GridHandler.
Usage:
    python -m GridHandler --config path/to/config.json
"""

import sys
import json
import logging
import argparse
from GridHandler import GridHandler

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# Main function
def main():
    parser = argparse.ArgumentParser(
        description="GridHandler CLI - download files from ALICE grid using JSON configuration."
    )
    parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="Path to the JSON configuration file",
    )
    args = parser.parse_args()

    config_path = args.config

    # Load JSON configuration
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"Failed to read config file: {config_path}\n{e}")
        sys.exit(1)

    # Initialize GridHandler
    try:
        handler = GridHandler.GridHandler(config)
    except Exception as e:
        logger.error(f"Failed to initialize GridHandler:\n{e}")
        sys.exit(1)

    # Start downloading
    try:
        handler.download()
    except Exception as e:
        logger.error(f"Download failed:\n{e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
