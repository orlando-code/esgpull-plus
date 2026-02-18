"""
Configuration paths for esgpullplus.

Locates the repository root by searching for search.yaml (works both
inside a git repo and from an installed package).
"""

from pathlib import Path
import subprocess


def get_repo_root() -> Path:
    """
    Find the repository root directory.

    Strategy:
    1. Try ``git rev-parse --show-toplevel`` from this file's directory.
    2. Walk up the directory tree looking for ``search.yaml``.
    3. Fall back to known hard-coded paths.
    """
    file_dir = Path(__file__).parent.resolve()

    # 1. Try git
    try:
        git_root = subprocess.run(
            ["git", "-C", str(file_dir), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if git_root.returncode == 0:
            candidate = Path(git_root.stdout.strip())
            if (candidate / "search.yaml").exists():
                return candidate
    except (OSError, subprocess.TimeoutExpired):
        pass

    # 2. Walk up the directory tree
    current = file_dir
    for _ in range(15):
        if (current / "search.yaml").exists():
            return current
        parent = current.parent
        if parent == current:
            break
        current = parent

    # 3. Hard-coded fallbacks
    for candidate in [
        Path("/maps/rt582/esgf-download"),
        Path("/maps-priv/maps/rt582/esgf-download"),
    ]:
        if (candidate / "search.yaml").exists():
            return candidate

    # Last resort: return the top-level package parent
    return file_dir.parent.parent


repo_dir = get_repo_root()
log_dir = repo_dir / "logs"
search_criteria_fp = repo_dir / "search.yaml"
plots_dir = repo_dir / "plots"

if __name__ == "__main__":
    print(repo_dir)