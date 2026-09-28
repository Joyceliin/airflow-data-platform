"""Torna `airflow/libs` importavel, reproduzindo o PYTHONPATH do worker."""

from __future__ import annotations

import sys
from pathlib import Path

LIBS_ROOT = Path(__file__).resolve().parent.parent
if str(LIBS_ROOT) not in sys.path:
    sys.path.insert(0, str(LIBS_ROOT))
