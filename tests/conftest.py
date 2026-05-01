import sys
from pathlib import Path

# pytest discovers and runs tests from the tests/ directory, which sits outside src/.
# Without this, `import transform`, `import extract`, etc. would raise ModuleNotFoundError.
# sys.path.insert(0, ...) adds src/ at the front so it takes priority over any installed packages.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
