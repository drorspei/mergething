#!/usr/bin/env python3
"""
CLI for mergething - IPython history merging tool
"""
import argparse
import shutil
import socket
import time
from pathlib import Path

from .ipython import merge_histories, cleanup_old_files


def merge_command(args):
    """Handle the merge command"""
    source_files = [Path(f) for f in args.sources]
    target_file = Path(args.target)

    # Validate source files exist
    for source in source_files:
        if not source.exists():
            print(f"Error: Source file {source} does not exist")
            return 1

    merge_histories(source_files, target_file)
    return 0


def cleanup_command(args):
    """Handle the cleanup command"""
    # Get the sync directory
    sync_dir = Path(args.directory).expanduser()
    
    if not sync_dir.exists():
        print(f"Error: Directory {sync_dir} does not exist")
        return 1
    
    # Convert minutes to seconds
    max_age_seconds = args.minutes * 60
    hostname = socket.gethostname()
    
    # Run cleanup
    cleanup_old_files(sync_dir, hostname, Path(), max_age_seconds, verbose=True)
    return 0


def init_command(args):
    """Handle the init command"""
    # Determine source file
    if args.source:
        source_file = Path(args.source).expanduser()
    else:
        source_file = Path("~/.ipython/profile_default/history.sqlite").expanduser()

    if not source_file.exists():
        print(f"Error: History file {source_file} does not exist")
        return 1

    # Prepare target directory
    target_dir = Path(args.target_directory).expanduser()
    target_dir.mkdir(parents=True, exist_ok=True)

    # Generate filename with completed pattern
    hostname = socket.gethostname()
    timestamp = int(time.time())
    target_file = target_dir / f"ipython_history_{hostname}_0_{timestamp}.db"

    # Copy the file
    shutil.copy2(source_file, target_file)
    print(f"Copied history to {target_file}")
    
    # Create a marker file to indicate this file is completed
    marker_file = target_dir / f"{target_file.name}.completed"
    marker_file.touch()
    print(f"Created completion marker: {marker_file}")

    # Handle ipython_config.py
    if args.config:
        config_file = Path(args.config).expanduser()
    else:
        config_file = Path("~/.ipython/profile_default/ipython_config.py").expanduser()

    # Check if config file exists and update it
    config_lines = f'''
try:
    from mergething.ipython import sync_and_get_hist_file
    c.HistoryManager.hist_file = sync_and_get_hist_file("{target_dir}", verbose=False)
except Exception:
    print("mergething: Error syncing and getting history file, using default ipython behavior")
'''

    if config_file.exists():
        # Read existing config
        with open(config_file, 'r') as f:
            content = f.read()

        # Check if mergething config already exists
        if "from mergething.ipython import sync_and_get_hist_file" not in content:
            # Append our configuration
            with open(config_file, 'a') as f:
                f.write(config_lines)
            print(f"Updated {config_file} with mergething configuration")
        else:
            print(f"Config file {config_file} already contains mergething configuration")
    else:
        # Create new config file with our lines
        config_file.parent.mkdir(parents=True, exist_ok=True)
        with open(config_file, 'w') as f:
            f.write(config_lines.lstrip())
        print(f"Created {config_file} with mergething configuration")

    return 0


def main():
    """Main entry point for the CLI"""
    parser = argparse.ArgumentParser(
        description="Merge IPython history files for syncing across devices"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Merge command
    merge_parser = subparsers.add_parser(
        "merge",
        help="Merge multiple history files into a target file"
    )
    merge_parser.add_argument(
        "sources",
        nargs="+",
        help="Source history files to merge"
    )
    merge_parser.add_argument(
        "target",
        help="Target file to merge histories into"
    )

    # Init command
    init_parser = subparsers.add_parser(
        "init",
        help="Initialize a sync directory with current IPython history and configure IPython"
    )
    init_parser.add_argument(
        "target_directory",
        help="Target directory to copy history file to and use for syncing"
    )
    init_parser.add_argument(
        "--source",
        default=None,
        help="Path to current IPython history file (default: ~/.ipython/profile_default/history.sqlite)"
    )
    init_parser.add_argument(
        "--config",
        default=None,
        help="Path to IPython config file (default: ~/.ipython/profile_default/ipython_config.py)"
    )
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser(
        "cleanup",
        help="Clean up old history files from the current host"
    )
    cleanup_parser.add_argument(
        "directory",
        help="Sync directory containing history files"
    )
    cleanup_parser.add_argument(
        "minutes",
        type=int,
        help="Remove files older than this many minutes"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == "merge":
        return merge_command(args)
    elif args.command == "init":
        return init_command(args)
    elif args.command == "cleanup":
        return cleanup_command(args)


if __name__ == "__main__":
    exit(main())
