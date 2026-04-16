from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"


def list_csv_files():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return sorted([f.name for f in DATA_DIR.glob("*.csv")])


def load_csv(file_name: str) -> pd.DataFrame:
    file_path = DATA_DIR / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_name}")

    encodings = ["utf-8-sig", "utf-8", "cp1251"]
    last_error = None

    for enc in encodings:
        try:
            return pd.read_csv(file_path, encoding=enc)
        except Exception as e:
            last_error = e

    raise RuntimeError(f"Failed to read CSV {file_name}: {last_error}")