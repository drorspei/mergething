
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


try:
    from line_profiler import profile
except ImportError:
    def profile(func):
        return func


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


@profile
def merge_histories(source_files: List[Path], target_file: Path, nthreads=2, verbose: bool = True) -> None:
    """Merge SQLite history files preserving session integrity and chronological order"""
    from collections import defaultdict

    # Create target database with IPython's exact schema
    target_conn = sqlite3.connect(str(target_file))
    target_conn.execute("PRAGMA journal_mode=OFF")
    target_conn.execute("PRAGMA synchronous=OFF")

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
    # Collect unique sessions: (source_file, orig_session_id, metadata)
    sessions_to_insert = []

    @profile
    def _read_file(source_file):
        """Read file and compute session signatures via GROUP BY (single table scan).
        GROUP_CONCAT runs in SQLite's C engine (GIL released), enabling
        true parallelism across threads."""
        conn = sqlite3.connect(":memory:")
        with open(str(source_file), "rb") as f:
            conn.deserialize(f.read())
        has_out = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='output_history'"
        ).fetchone() is not None

        sessions = conn.execute(
            'SELECT session, start, end, num_cmds, remark FROM sessions ORDER BY session DESC'
        ).fetchall()

        # Single table scan with GROUP BY — much faster than per-session subqueries
        hist_sigs = dict(conn.execute(
            "SELECT session, GROUP_CONCAT(line || char(31) || source || char(31) || COALESCE(source_raw, ''), char(30)) FROM history GROUP BY session"
        ).fetchall())

        out_sigs = {}
        if has_out:
            out_sigs = dict(conn.execute(
                "SELECT session, GROUP_CONCAT(line || char(31) || output, char(30)) FROM output_history GROUP BY session"
            ).fetchall())
        conn.close()

        return [(s[0], s[1:], (hist_sigs.get(s[0]), out_sigs.get(s[0]))) for s in sessions]

    # Read all files in parallel threads. GROUP BY + GROUP_CONCAT runs in
    # SQLite's C engine which releases the GIL, enabling true parallelism.
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=min(nthreads, len(files_with_times))) as executor:
        futures = [
            (source_file, executor.submit(_read_file, source_file))
            for _, source_file in files_with_times
        ]

    # Dedup in order (files already sorted newest-first)
    for source_file, future in futures:
        try:
            file_sessions = future.result()
        except Exception as e:
            if verbose:
                print(f"mergething: Warning: Could not read {source_file}: {e}")
            continue

        for session_id, metadata, signature in file_sessions:
            if signature in seen_sessions:
                continue
            seen_sessions.add(signature)
            sessions_to_insert.append((source_file, session_id, metadata))

    # Sort sessions chronologically
    sessions_to_insert.sort(key=lambda d: d[2][1] or d[2][0])

    # Group by source file, preserving new session IDs
    sessions_by_file = defaultdict(list)
    for new_id, (source_file, orig_session, metadata) in enumerate(sessions_to_insert, 1):
        sessions_by_file[source_file].append((orig_session, new_id, metadata))

    # Use ATTACH + INSERT...SELECT to write data without passing through Python
    target_conn.execute('''
        CREATE TEMP TABLE session_map (source_session INTEGER, target_session INTEGER)
    ''')

    for source_file, session_list in sessions_by_file.items():
        target_conn.execute("ATTACH DATABASE ? AS src", (str(source_file),))

        # Insert session metadata
        target_conn.executemany(
            'INSERT INTO sessions (session, start, end, num_cmds, remark) VALUES (?, ?, ?, ?, ?)',
            [(new_id, *meta) for _, new_id, meta in session_list]
        )

        # Populate mapping table
        target_conn.execute("DELETE FROM session_map")
        target_conn.executemany(
            'INSERT INTO session_map VALUES (?, ?)',
            [(orig, new_id) for orig, new_id, _ in session_list]
        )

        # Bulk copy history via SQL (data stays in SQLite, never passes through Python)
        target_conn.execute('''
            INSERT INTO history (session, line, source, source_raw)
            SELECT m.target_session, h.line, h.source, h.source_raw
            FROM src.history h
            JOIN session_map m ON h.session = m.source_session
        ''')

        # Bulk copy output_history if it exists in source
        has_output = target_conn.execute(
            "SELECT name FROM src.sqlite_master WHERE type='table' AND name='output_history'"
        ).fetchone()
        if has_output:
            target_conn.execute('''
                INSERT INTO output_history (session, line, output)
                SELECT m.target_session, o.line, o.output
                FROM src.output_history o
                JOIN session_map m ON o.session = m.source_session
            ''')

        target_conn.commit()
        target_conn.execute("DETACH DATABASE src")

    target_conn.execute("DROP TABLE session_map")
    target_conn.commit()
    target_conn.close()
    # Ensure all data is fsynced to disk (writes above used synchronous=OFF for speed)
    fd = os.open(str(target_file), os.O_RDONLY)
    os.fsync(fd)
    os.close(fd)
    if verbose:
        print(f"mergething: Merged {len(files_with_times)} history files into {len(sessions_to_insert)} sessions")


def cleanup_old_files(sync_dir: Path, hostname: str, current_file: Path, safe_files: List[Path], verbose: bool = True) -> None:
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
        if f == current_file or f not in safe_files:
            continue

        try:
            for f in sync_dir.glob(f'{file_path.name[:-len(".completed")]}*'):
                f.unlink()
        except (ValueError, IndexError, OSError):
            continue


def sync_and_get_hist_file(sync_dir: Union[str, Path] = "~/syncthing/ipython_history", nthreads=2, verbose: bool = False, hostname: Optional[str] = None) -> str:
    """
    Set up synchronized IPython history across multiple machines.

    Args:
        sync_dir: Directory where history files are synced (default: ~/syncthing/ipython_history
        nthreads: Max number of threads to use)
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
        merge_histories(safe_files, current_file, nthreads=nthreads, verbose=verbose)
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
            cleanup_old_files(sync_dir, hostname, current_file, safe_files, verbose=verbose)
        except Exception as e:
            if verbose:
                print(f"mergething: Warning: Could not create completion marker or cleanup on exit: {e}")

    atexit.register(cleanup_on_exit)

    return str(current_file)
