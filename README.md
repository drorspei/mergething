# mergething

App over syncthing that takes care merging things

Install using `pip install mergething`

## ipython

Currently supports merging ipython history files.

# IPython History Sync

## 1. Configure Syncthing

Set up Syncthing to sync a directory across your machines (e.g., `~/syncthing/ipython_history`).

## 2a. Configure IPython using CLI tool

The following line will copy your exisiting history file to the directory and add lines to you `ipython_config.py` file to use mergething:

`mergething init ~/syncthing/ipython_history`

## 2b. Configure IPython manually

Alternatively, you can add these lines to the end of your IPython configuration file manually (`~/.ipython/profile_default/ipython_config.py`):

```python
try:
    from mergething.ipython import sync_and_get_hist_file
    c.HistoryManager.hist_file = sync_and_get_hist_file("~/syncthing/ipython_history", verbose=False)
except Exception:
    print("mergething: Error syncing and getting history file, using default ipython behavior")
```

## Merging existing files

You can use the CLI tool to merge existing files:

`mergething merge history1.sqlite history2.sqlite ... historyn.sqlite merged_history.sqlite`
