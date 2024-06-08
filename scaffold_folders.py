

from pathlib import Path

NEEDED_FOLDERS = [
    './data/mart',
    './data/ml_models',
    './data/ml',
    './data/raw',
    './data/staging',
]

def create_folders():
    for folder in NEEDED_FOLDERS:
        Path(folder).mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    create_folders()