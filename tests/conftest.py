import os
import sys
from pathlib import Path


def pytest_sessionstart(session):
    # Ensure the project root (containing the 'margana_score' package) is importable
    here = Path(__file__).resolve()
    project_root = here.parent.parent  # .../python
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    # Also add parent of python dir if needed
    parent = project_root.parent
    if parent and str(parent) not in sys.path:
        sys.path.insert(0, str(parent))
