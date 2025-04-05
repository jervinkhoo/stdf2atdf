#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Alternative runner script for the STDF to ATDF converter.

This script provides a way to execute the converter directly
using `python run_conversion.py ...` instead of `python -m src ...`.
"""

import sys
# No longer need os for path manipulation here

try:
    # Import main directly from the src package
    from src.cli import main
except ImportError as e:
    print(f"Error: Could not import the main function from src.cli.", file=sys.stderr)
    print(f"Details: {e}", file=sys.stderr)
    # Adding a note about running from the correct directory
    print("Ensure the script is run from the project root directory (the directory containing 'run_conversion.py' and 'src/').", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    """
    Entry point when the script is executed directly.
    Calls the main function from src.cli and exits with its return code.
    """
    # The main function from src.cli handles argument parsing
    # and returns an exit code (0 for success, non-zero for failure).
    # We capture the exit code and exit the script with it.
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
        # Catch any unexpected exceptions from main() itself
        print(f"An unexpected error occurred during execution: {e}", file=sys.stderr)
        sys.exit(1) # Exit with a generic error code
