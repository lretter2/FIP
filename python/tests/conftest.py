import os
import sys
from types import ModuleType
from unittest.mock import MagicMock

# Make the Python source directory importable from all test files
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Anomaly_detection"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Commentary"))

# anomaly_detector.py raises EnvironmentError at import time when this is unset;
# set a dummy value so the module can be imported in the test environment.
os.environ.setdefault("AZURE_KEY_VAULT_URL", "https://test-vault.vault.azure.net/")

# Provide a comprehensive db_utils stub so all modules that import from it
# (anomaly_detector, commentary_generator, etc.) work without real Azure infra.
# Using sys.modules.setdefault so individual test files that set up their own
# stubs still take precedence when they run standalone, but this ensures a
# complete stub is always available during full-suite collection.
_db_utils_stub = sys.modules.get("db_utils")
if _db_utils_stub is None:
    _db_utils_stub = ModuleType("db_utils")
    sys.modules["db_utils"] = _db_utils_stub
if not hasattr(_db_utils_stub, "get_db_connection"):
    _db_utils_stub.get_db_connection = MagicMock()  # type: ignore[attr-defined]
if not hasattr(_db_utils_stub, "get_openai_client"):
    _db_utils_stub.get_openai_client = MagicMock()  # type: ignore[attr-defined]
