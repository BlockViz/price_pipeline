# paths.py
from __future__ import annotations
from pathlib import Path
import os

# Repo root is where this file lives
_REPO_ROOT = Path(__file__).resolve().parent

def repo_root() -> Path:
    """Absolute Path to the backend repository root."""
    return _REPO_ROOT

def rel(*parts: str) -> Path:
    """Path resolved from the backend repo root."""
    return _REPO_ROOT.joinpath(*parts)

def chdir_repo_root() -> None:
    """Optionally ensure the current working directory is the repo root."""
    if Path.cwd() != _REPO_ROOT:
        os.chdir(_REPO_ROOT)
