import argparse
from pathlib import Path

from datasets import load_dataset


def main():
    parser = argparse.ArgumentParser(description="Download the AG News training split")
    parser.add_argument(
        "--output",
        default="data/ag_news/train.parquet",
        help="Destination path for the training Parquet file",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    dataset = load_dataset("fancyzhx/ag_news", split="train")
    print("Rows:", len(dataset))
    print("Columns:", dataset.column_names)
    dataset.to_parquet(output_path)
    print(f"Saved successfully to {output_path}")


if __name__ == "__main__":
    main()
