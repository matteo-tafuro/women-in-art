import argparse

import pandas as pd
import datasets
import os


def get_wikiart_metadata(output_path):

    if not output_path:
        # Get the absolute path of the current file
        absolute_path = os.path.abspath(__file__)

        # Get the directory containing the current file
        script_directory = os.path.dirname(absolute_path)

        output_dir = os.path.join(script_directory, "output")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_path = os.path.join(output_dir, "wikiart_metadata.csv")

    # Get dataset from hugging face
    wikiart = datasets.load_dataset("Artificio/WikiArt")

    # Export to csv
    wikiart["train"].to_csv("tmp_wikiart.csv")

    # import csv
    wikiart_df = pd.read_csv("tmp_wikiart.csv")

    # Remove "image" and "embeddings_pca512" columns
    wikiart_df = wikiart_df.drop(columns=["image", "embeddings_pca512"])

    # Save the dataframe to a new csv file
    print("Saving the metadata to a new CSV file...")
    wikiart_df.to_csv(output_path, index=False)

    # Remove the tmp file
    os.remove("tmp_wikiart.csv")

    print("Done!")

    return wikiart_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download metadata from the WikiArt dataset on Hugging Face and save it to a CSV file."
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=None,
        help="Path to the output directory where the CSV file will be saved.",
    )

    args = parser.parse_args()
    get_wikiart_metadata(args.output_dir)
