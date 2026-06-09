"""
Pytest configuration file to handle module imports.

Ensures the project root is in sys.path so our local 'queue' package
takes precedence over the built-in 'queue' module.
"""

import sys
from pathlib import Path

# Add project root to sys.path at the beginning
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))

# Remove any built-in queue module that may have been imported
if 'queue' in sys.modules:
    del sys.modules['queue']
