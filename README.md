# mergething

App over syncthing that takes care merging things

Install using `pip install mergething`

## ipython

Currently supports merging ipython history files.

# IPython History Sync

Sync your IPython/Jupyter history across multiple machines using Syncthing (or any file sync service).

## Features

- 🔄 **Automatic merging** of IPython history from multiple machines
- 🛡️ **Safe concurrent access** - no database corruption from multiple processes
- 📚 **Session preservation** - maintains complete session integrity
- 🚀 **Zero configuration** - works with existing Syncthing setups
- 🧹 **Automatic cleanup** - removes old history files
- ⚡ **Fast startup** - efficient merging algorithm

## Installation

```bash
pip install ipython-history-sync
```

## Setup

### 1. Configure Syncthing

Set up Syncthing to sync a directory across your machines (e.g., `~/syncthing/ipython_history`).

### 2. Configure IPython

Add these lines to the end of your IPython configuration file (`~/.ipython/profile_default/ipython_config.py`):

```python
try:
    from mergething.ipython import sync_and_get_hist_file
    c.HistoryManager.hist_file = sync_and_get_hist_file("~/my_custom_sync_dir", verbose=False)
except Exception:
    print("mergething: Error syncing and getting history file, using default ipython behavior")
```
