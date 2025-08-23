
"""
IPython History Sync - Sync IPython history across multiple machines
"""
import os
import sqlite3
import time
import socket
import shutil
import atexit
from pathlib import Path
from typing import List, Union


def get_safe_files_for_merge(sync_dir: Path, current_file: Path) -> List[Path]:
    """Get files that are definitely safe to read"""
    safe_files = []
    current_hostname = socket.gethostname()

    # 1. All _completed files (these are guaranteed safe)
    completed_files = list(sync_dir.glob("ipython_history_*_completed.db"))
    safe_files.extend(completed_files)

    # 2. Regular files from other machines (safe due to Syncthing atomicity)
    for file_path in sync_dir.glob("ipython_history_*.db"):
        if file_path == current_file:
            continue
        if "_completed" in file_path.name:
            continue  # Already included above

        try:
            # Parse hostname from filename: ipython_history_{hostname}_{pid}_{timestamp}.db
            parts = file_path.stem.split('_')
            if len(parts) >= 4:
                hostname = parts[2]
                if hostname != current_hostname:
                    safe_files.append(file_path)
        except (ValueError, IndexError):
            continue

    # Sort files by (is_this_machine, timestamp) in reverse order
    # This puts this machine's files first, and within each machine, newest files first
    def sort_key(file_path):
        try:
            # Extract hostname and timestamp from filename
            parts = file_path.stem.replace('_completed', '').split('_')
            if len(parts) >= 4:
                hostname = parts[2]
                timestamp = int(parts[-1])
                is_this_machine = (hostname == current_hostname)
                # Return tuple for sorting: (is_this_machine, timestamp)
                # We negate is_this_machine so True (1) comes before False (0) when reversed
                return (is_this_machine, timestamp)
        except (ValueError, IndexError):
            # Fallback for files that don't match the expected pattern
            return (False, 0)
    
    safe_files.sort(key=sort_key, reverse=True)
    
    return safe_files


def merge_histories(source_files: List[Path], target_file: Path, verbose: bool = True) -> None:
    """Merge SQLite history files preserving session integrity and chronological order"""

    # Create target database with IPython's exact schema
    target_conn = sqlite3.connect(str(target_file))

    # Use IPython's exact table definitions
    target_conn.execute('''
        CREATE TABLE IF NOT EXISTS sessions
        (session integer primary key autoincrement, start timestamp,
         end timestamp, num_cmds integer, remark text)
    ''')
    target_conn.execute('''
        CREATE TABLE IF NOT EXISTS history
        (session integer, line integer, source text, source_raw text,
         PRIMARY KEY (session, line))
    ''')
    target_conn.execute('''
        CREATE TABLE IF NOT EXISTS output_history
        (session integer, line integer, output text,
         PRIMARY KEY (session, line))
    ''')

    # Sort files by creation time for chronological ordering
    files_with_times = []
    for source_file in source_files:
        try:
            # Extract timestamp from filename
            parts = Path(source_file).stem.replace('_completed', '').split('_')
            timestamp = int(parts[-1])
            files_with_times.append((timestamp, source_file))
        except (ValueError, IndexError):
            # Fallback to file mtime
            try:
                timestamp = int(Path(source_file).stat().st_mtime)
                files_with_times.append((timestamp, source_file))
            except OSError:
                continue

    # Sort by timestamp (oldest first)
    files_with_times.sort(key=lambda x: x[0])

    # Track seen sessions using tuple of all commands + outputs
    seen_sessions = set()
    next_session_id = 1

    for timestamp, source_file in files_with_times:
        try:
            source_conn = sqlite3.connect(str(source_file))

            # Check if output_history table exists
            cursor = source_conn.execute('''
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='output_history'
            ''')
            has_output_history = cursor.fetchone() is not None

            # Get all sessions from this file
            sessions_cursor = source_conn.execute('''
                SELECT session, start, end, num_cmds, remark
                FROM sessions
                ORDER BY session
            ''')

            for session_row in sessions_cursor:
                orig_session, start_time, end_time, num_cmds, remark = session_row

                # Get all commands for this session
                history_cursor = source_conn.execute('''
                    SELECT line, source, source_raw
                    FROM history
                    WHERE session = ?
                    ORDER BY line
                ''', (orig_session,))

                commands = list(history_cursor)

                # Get all outputs for this session (if table exists)
                outputs = []
                if has_output_history:
                    output_cursor = source_conn.execute('''
                        SELECT line, output
                        FROM output_history
                        WHERE session = ?
                        ORDER BY line
                    ''', (orig_session,))
                    outputs = list(output_cursor)

                # Create session signature: tuple of commands + outputs
                commands_tuple = tuple(
                    (line, source or "", source_raw or "")
                    for line, source, source_raw in commands
                )
                outputs_tuple = tuple(
                    (line, output or "")
                    for line, output in outputs
                )
                session_signature = (commands_tuple, outputs_tuple)

                # Skip if we've seen this exact session before
                if session_signature in seen_sessions:
                    continue

                seen_sessions.add(session_signature)

                # Insert session metadata
                target_conn.execute('''
                    INSERT INTO sessions (session, start, end, num_cmds, remark)
                    VALUES (?, ?, ?, ?, ?)
                ''', (next_session_id, start_time, end_time, num_cmds, remark))

                # Insert all commands for this session
                for line_num, source, source_raw in commands:
                    target_conn.execute('''
                        INSERT INTO history (session, line, source, source_raw)
                        VALUES (?, ?, ?, ?)
                    ''', (next_session_id, line_num, source, source_raw))

                # Insert all outputs for this session
                for line_num, output in outputs:
                    target_conn.execute('''
                        INSERT INTO output_history (session, line, output)
                        VALUES (?, ?, ?)
                    ''', (next_session_id, line_num, output))

                next_session_id += 1

            source_conn.close()

        except sqlite3.Error as e:
            if verbose:
                print(f"mergething: Warning: Could not read {source_file}: {e}")
            continue

    target_conn.commit()
    target_conn.close()
    if verbose:
        print(f"mergething: Merged {len(files_with_times)} history files into {next_session_id - 1} sessions")


def cleanup_old_files(sync_dir: Path, hostname: str, current_file: Path, max_age_seconds: int = 300, verbose: bool = True) -> None:
    """Clean up old files from this machine"""
    cutoff_time = time.time() - max_age_seconds

    for pattern in [f"ipython_history_{hostname}_*.db", f"ipython_history_{hostname}_*_completed.db"]:
        for file_path in sync_dir.glob(pattern):
            if file_path == current_file:
                continue

            try:
                # Extract timestamp (handle both regular and completed files)
                parts = file_path.stem.replace('_completed', '').split('_')
                file_timestamp = int(parts[-1])

                if file_timestamp < cutoff_time:
                    file_path.unlink()
                    if verbose:
                        print(f"mergething: Cleaned up old history file: {file_path}")

            except (ValueError, IndexError, OSError):
                continue


def sync_and_get_hist_file(sync_dir: Union[str, Path] = "~/syncthing/ipython_history", verbose: bool = False) -> str:
    """
    Set up synchronized IPython history across multiple machines.

    Args:
        sync_dir: Directory where history files are synced (default: ~/syncthing/ipython_history)
        verbose: Whether to print status messages (default: True)

    Returns:
        Path to the history file for this IPython session
    """
    sync_dir = Path(sync_dir).expanduser()
    sync_dir.mkdir(parents=True, exist_ok=True)

    hostname = socket.gethostname()
    pid = os.getpid()
    timestamp = int(time.time())
    current_file = sync_dir / f"ipython_history_{hostname}_{pid}_{timestamp}.db"

    # Merge from safe files only
    safe_files = get_safe_files_for_merge(sync_dir, current_file)

    if safe_files:
        if verbose:
            print(f"mergething: Merging {len(safe_files)} history files...")
        merge_histories(safe_files, current_file, verbose=verbose)
    else:
        if verbose:
            print("mergething: No existing history files found, starting fresh.")

    # Create completed copy for other processes to use
    completed_file = sync_dir / f"ipython_history_{hostname}_{timestamp}_completed.db"
    if current_file.exists():
        shutil.copy2(current_file, completed_file)

    # Clean up old files from this machine
    cleanup_old_files(sync_dir, hostname, current_file, verbose=verbose)

    # Register cleanup on exit
    def cleanup_on_exit():
        try:
            # Update the completed copy on exit
            if current_file.exists():
                shutil.copy2(current_file, completed_file)
                if verbose:
                    print(f"mergething: Updated completed history file: {completed_file}")
        except Exception as e:
            if verbose:
                print(f"mergething: Warning: Could not update completed file on exit: {e}")

    atexit.register(cleanup_on_exit)

    return str(current_file)
