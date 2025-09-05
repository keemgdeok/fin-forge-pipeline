import os
import sys


def pytest_sessionstart(session):
    # Ensure handlers and shared layer are importable during tests
    functions_root = os.path.abspath("src/lambda/functions")
    if functions_root not in sys.path:
        sys.path.insert(0, functions_root)
    # Add layer python path so 'shared' package resolves locally like in Lambda (/opt/python)
    layer_python = os.path.abspath("src/lambda/layers/common/python")
    if layer_python not in sys.path:
        sys.path.insert(0, layer_python)
