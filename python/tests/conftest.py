import os
import sys

# Make the Python source directory importable from all test files
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "Anomaly_detection"))

# anomaly_detector.py raises EnvironmentError at import time when this is unset;
# set a dummy value so the module can be imported in the test environment.
os.environ.setdefault("AZURE_KEY_VAULT_URL", "https://test-vault.vault.azure.net/")
