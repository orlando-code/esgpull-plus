from pathlib import Path
import time
from typing import Optional
from rich.console import Console


def get_repo_root():
    """Get the repository root directory by searching for search.yaml.
    
    When running from site-packages, searches up the directory tree more thoroughly
    to find the actual repository root.
    """
    # Start from the file's directory
    current = Path(__file__).resolve().parent
    
    # If we're in site-packages, we need to search much higher up
    # Check if we're in a site-packages-like location
    is_installed = 'site-packages' in str(current) or 'dist-packages' in str(current)
    max_depth = 20 if is_installed else 10  # Search deeper if installed
    
    # Search up the directory tree for search.yaml
    for _ in range(max_depth):
        search_yaml = current / "search.yaml"
        if search_yaml.exists():
            return current
        parent = current.parent
        if parent == current:  # Reached filesystem root
            break
        current = parent
    
    # If not found, try common repository locations
    # Check if there's a known repo path we can use
    possible_repos = [
        Path("/maps/rt582/esgf-download"),
        Path("/maps-priv/maps/rt582/esgf-download"),
        Path(__file__).resolve().parent.parent.parent.parent.parent,  # Go up from site-packages/esgpull/esgpullplus
    ]
    
    for repo_path in possible_repos:
        if (repo_path / "search.yaml").exists():
            return repo_path
    
    # Last resort: return the esgpull package location (but this shouldn't happen)
    return Path(__file__).resolve().parent.parent.parent


def read_yaml(file_path):
    """Read a YAML file, returning None if it doesn't exist."""
    import yaml
    
    try:
        with open(file_path, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        return None


# Try to load search.yaml, but don't fail if it doesn't exist
REPO_ROOT = get_repo_root()
CRITERIA_FP = REPO_ROOT / "search.yaml"
CRITERIA = read_yaml(CRITERIA_FP)

# Provide defaults if file doesn't exist
if CRITERIA is None:
    CRITERIA = {}
    SEARCH_CRITERIA_CONFIG = {}
    META_CRITERIA_CONFIG = {}
else:
    SEARCH_CRITERIA_CONFIG = CRITERIA.get("search_criteria", {})
    META_CRITERIA_CONFIG = CRITERIA.get("meta_criteria", {})


def print_timestamp(console: Console, message: str = "START") -> time.struct_time:
    """Print a standardized START timestamp using a Rich-like console.

    Args:
        console (Console): the console to print the start timestamp to
        message (str): the message to print with the start timestamp
    """
    timestamp = time.localtime()
    console.print(
        f":clock3: {message}: {time.strftime('%Y-%m-%d %H:%M:%S', timestamp)}\n"
    )
    return timestamp


def get_processing_time(start_time: time.struct_time, end_time: Optional[time.struct_time] = None) -> float:
    """Get the processing time in seconds.

    Args:
        start_time (time.struct_time): the start time
        end_time (time.struct_time): the end time

    Returns (float): the processing time in seconds
    """
    if end_time is None:
        end_time = time.localtime()
    return time.mktime(end_time) - time.mktime(start_time)


def format_processing_time(processing_time: Optional[float] = None) -> str:
    """
    Format the processing time in seconds into a human-readable string.

    Args:
        processing_time (Optional[float]): the processing time in seconds

    Returns (str): the processing time in a human-readable string
    """
    if processing_time is None:
        return "Timing not available"
    
    hours = int(processing_time // 3600)
    minutes = int((processing_time % 3600) // 60)
    seconds = int(processing_time % 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"
    