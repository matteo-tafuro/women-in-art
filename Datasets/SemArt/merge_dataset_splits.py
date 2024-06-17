import argparse
import os.path

import pandas as pd


def merge_splits(root, out_path):

    train_path = os.path.join(root, "semart_train.csv")
    test_path = os.path.join(root, "semart_test.csv")
    val_path = os.path.join(root, "semart_val.csv")

    # Load the data from CSV files
    train_df = pd.read_csv(train_path, encoding="latin-1", sep="	")
    print(f"Number of rows in train split: {len(train_df)}")

    test_df = pd.read_csv(test_path, encoding="latin-1", sep="	")
    print(f"Number of rows in test split: {len(test_df)}")

    val_df = pd.read_csv(val_path, encoding="latin-1", sep="	")
    print(f"Number of rows in val split: {len(val_df)}")

    # Add a 'SPLIT' column to each dataframe
    train_df["SPLIT"] = "train"
    test_df["SPLIT"] = "test"
    val_df["SPLIT"] = "val"

    # Merge the dataframes
    merged_df = pd.concat([train_df, test_df, val_df], ignore_index=True)
    print(f"Number of rows in merged dataframe: {len(merged_df)}")

    # Save the merged dataframe to a new CSV file
    merged_df.to_csv(out_path, index=False)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Merge the three splits of the SemArt dataset."
    )
    parser.add_argument(
        "--root",
        type=str,
        required=True,
        help="Path to the directory containing the three dataset splits.",
    )

    parser.add_argument(
        "--output_dir",
        type=str,
        default=None,
        help="Path to the output directory where the CSV file will be saved.",
    )

    args = parser.parse_args()

    if not args.output_dir:
        output_dir = os.path.join(os.path.dirname(args.root), "output")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        args.output_dir = os.path.join(output_dir, "semart_merged.csv")

    merge_splits(root=args.root, out_path=args.output_dir)
