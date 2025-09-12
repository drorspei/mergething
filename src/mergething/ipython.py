
"""
IPython History Sync - Sync IPython history across multiple machines
"""
import os
import sqlite3
import time
import socket
import atexit
import platform
import subprocess
from pathlib import Path
from typing import List, Optional, Union


def is_process_running(pid: int) -> bool:
    """Check if a process with given PID is still running"""
    system = platform.system()

    try:
        if system in ['Linux', 'Android']:
            # Check /proc/{pid} on Linux/Android
            return Path(f'/proc/{pid}').exists()
        elif system == 'Darwin':  # macOS
            # Use os.kill with signal 0 to check if process exists
            try:
                os.kill(pid, 0)
                return True
            except (OSError, ProcessLookupError):
                return False
        elif system == 'Windows':
            # Use tasklist command on Windows
            try:
                result = subprocess.run(
                    ['tasklist', '/FI', f'PID eq {pid}'],
                    capture_output=True,
                    text=True,
                    check=False
                )
                return str(pid) in result.stdout
            except (subprocess.SubprocessError, FileNotFoundError):
                # If we can't run tasklist, assume process is running
                return True
        else:
            # Unknown system, assume process is running to be safe
            return True
    except Exception:
        # If we can't determine, assume process is running to be safe
        return True


def get_safe_files_for_merge(sync_dir: Path, current_file: Path) -> List[Path]:
    """Get files that are definitely safe to read"""
    safe_files = []
    current_hostname = socket.gethostname()

    # 1. Files that have a .completed marker (these are guaranteed safe)
    for marker_file in sync_dir.glob("ipython_history_*.db.completed"):
        # Get the original file name by removing .completed suffix
        original_file = sync_dir / marker_file.name.replace(".completed", "")
        if original_file.exists() and original_file != current_file:
            safe_files.append(original_file)

    # 2. Regular files from other machines (safe due to Syncthing atomicity)
    for file_path in sync_dir.glob("ipython_history_*.db"):
        if file_path == current_file:
            continue

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
            parts = file_path.stem.split('_')
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
            parts = Path(source_file).stem.split('_')
            timestamp = int(parts[-1])
            files_with_times.append((timestamp, source_file))
        except (ValueError, IndexError):
            # Fallback to file mtime
            try:
                timestamp = int(Path(source_file).stat().st_mtime)
                files_with_times.append((timestamp, source_file))
            except OSError:
                continue

    # Sort by timestamp (newest first for reverse processing)
    files_with_times.sort(key=lambda x: x[0], reverse=True)

    # Track seen sessions using tuple of all commands + outputs
    seen_sessions = set()
    # Collect all unique sessions in reverse order
    sessions_to_insert = []

    for timestamp, source_file in files_with_times:
        try:
            source_conn = sqlite3.connect(str(source_file))

            # Check if output_history table exists
            cursor = source_conn.execute('''
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='output_history'
            ''')
            has_output_history = cursor.fetchone() is not None

            # Get all sessions from this file in reverse order
            sessions_cursor = source_conn.execute('''
                SELECT session, start, end, num_cmds, remark
                FROM sessions
                ORDER BY session DESC
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
                # Since we're going in reverse, we keep the most recent version
                if session_signature in seen_sessions:
                    continue

                seen_sessions.add(session_signature)

                # Store the session data for later insertion
                sessions_to_insert.append({
                    'metadata': (start_time, end_time, num_cmds, remark),
                    'commands': commands,
                    'outputs': outputs
                })

            source_conn.close()

        except sqlite3.Error as e:
            if verbose:
                print(f"mergething: Warning: Could not read {source_file}: {e}")
            continue

    # Now insert sessions in chronological order (reverse of reverse order)
    sessions_to_insert.sort(key=lambda d: d['metadata'][1] or d['metadata'][0])

    next_session_id = 1
    for session_data in sessions_to_insert:
        start_time, end_time, num_cmds, remark = session_data['metadata']

        # Insert session metadata
        target_conn.execute('''
            INSERT INTO sessions (session, start, end, num_cmds, remark)
            VALUES (?, ?, ?, ?, ?)
        ''', (next_session_id, start_time, end_time, num_cmds, remark))

        # Insert all commands for this session
        for line_num, source, source_raw in session_data['commands']:
            target_conn.execute('''
                INSERT INTO history (session, line, source, source_raw)
                VALUES (?, ?, ?, ?)
            ''', (next_session_id, line_num, source, source_raw))

        # Insert all outputs for this session
        for line_num, output in session_data['outputs']:
            target_conn.execute('''
                INSERT INTO output_history (session, line, output)
                VALUES (?, ?, ?)
            ''', (next_session_id, line_num, output))

        next_session_id += 1

    target_conn.commit()
    target_conn.close()
    if verbose:
        print(f"mergething: Merged {len(files_with_times)} history files into {len(sessions_to_insert)} sessions")


def cleanup_old_files(sync_dir: Path, hostname: str, current_file: Path, verbose: bool = True) -> None:
    """Clean up old files from this machine and mark completed files from dead processes"""
    current_hostname = socket.gethostname()

    # First, check for files from dead processes and mark them as completed
    if hostname == current_hostname:
        for file_path in sync_dir.glob(f"ipython_history_{hostname}_*.db"):
            if file_path == current_file:
                continue

            try:
                # Parse the PID from filename: ipython_history_{hostname}_{pid}_{timestamp}.db
                parts = file_path.stem.split('_')
                if len(parts) >= 5:  # Has PID
                    pid = int(parts[3])

                    # Check if the process is still running
                    if not is_process_running(pid):
                        # Process is dead, mark the file as completed
                        marker_file = sync_dir / f"{file_path.name}.completed"
                        if not marker_file.exists():
                            marker_file.touch()
                            if verbose:
                                print(f"mergething: Marked completed (process {pid} dead): {file_path}")
            except (ValueError, IndexError):
                continue

    # Clean up old history files and their markers
    for file_path in sync_dir.glob(f"ipython_history_{hostname}_*.db.completed"):
        f = sync_dir / file_path.name[:-len(".completed")]
        if f == current_file:
            continue

        try:
            file_path.unlink()
            f.unlink()
        except (ValueError, IndexError, OSError):
            continue


def sync_and_get_hist_file(sync_dir: Union[str, Path] = "~/syncthing/ipython_history", verbose: bool = False, hostname: Optional[str] = None) -> str:
    """
    Set up synchronized IPython history across multiple machines.

    Args:
        sync_dir: Directory where history files are synced (default: ~/syncthing/ipython_history)
        verbose: Whether to print status messages (default: False)
        hostname: Hostname to use for file naming (default: socket.gethostname())
                 Useful on Android/Termux where hostname is always "localhost"

    Returns:
        Path to the history file for this IPython session
    """
    sync_dir = Path(sync_dir).expanduser()
    sync_dir.mkdir(parents=True, exist_ok=True)

    if hostname is None:
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

    # Register cleanup on exit
    def cleanup_on_exit():
        try:
            # Create an empty marker file to indicate this file is completed
            # This avoids conflicts with IPython's own history flushing
            marker_file = sync_dir / f"{current_file.name}.completed"
            marker_file.touch()
            if verbose:
                print(f"mergething: Created completion marker: {marker_file}")
            
            # Now clean up old files from this machine
            # This happens after marking completion to avoid race conditions
            cleanup_old_files(sync_dir, hostname, current_file, verbose=verbose)
        except Exception as e:
            if verbose:
                print(f"mergething: Warning: Could not create completion marker or cleanup on exit: {e}")

    atexit.register(cleanup_on_exit)

    return str(current_file)
