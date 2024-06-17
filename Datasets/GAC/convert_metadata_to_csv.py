import argparse
import xml.etree.ElementTree as ET
import pandas as pd
import os
import json
from tqdm import tqdm


def main(root, csv_path=None):

    if csv_path is None:
        output_dir = os.path.join(os.path.dirname(root), "output")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        csv_path = os.path.join(output_dir, "gac_metadata_final.csv")

    artists_directories = []
    outer_dirs = [d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
    for outer_dir in outer_dirs:
        inner_dirs = [
            d
            for d in os.listdir(os.path.join(root, outer_dir))
            if os.path.isdir(os.path.join(root, outer_dir, d))
        ]
        for inner_dir in inner_dirs:
            artists_directories.append(os.path.join(outer_dir, inner_dir))

    print(
        f"Found {len(artists_directories)} artists in the root directory! Starting processing..."
    )

    # Initialize an empty DataFrame
    gac_df = pd.DataFrame()

    artists_directories_tqdm = tqdm(artists_directories, desc="Processing artists...")
    for artist_dir in artists_directories_tqdm:
        artist_path = os.path.join(root, artist_dir)
        works_path = os.path.join(artist_path, "works")

        artist_name = " ".join(
            os.path.basename(os.path.normpath(artist_dir)).split("_")
        )

        if os.path.exists(works_path):

            artworks_directories = [
                directory
                for directory in os.listdir(works_path)
                if os.path.isdir(os.path.join(works_path, directory))
            ]

            desc_str = (
                f"Analyzing {len(artworks_directories)} artworks of {artist_name}..."
            )
            artists_directories_tqdm.set_description(desc_str)

            for artwork_dir in artworks_directories:
                artwork_path = os.path.join(works_path, artwork_dir)
                metadata_path = os.path.join(artwork_path, "metadata.json")

                if os.path.exists(metadata_path) and os.path.isfile(metadata_path):
                    with open(metadata_path, "r") as file:
                        metadata = json.load(file)

                    # Add the artist and artwork directories to the metadata
                    metadata["artwork_path"] = f"{artist_dir}/{artwork_dir}"

                    # We have "date" and "date_created". Merge them under "date"
                    if not metadata.get("date", None):
                        metadata["date"] = metadata.get("date created", None)
                    # Remove "date_created" key
                    metadata.pop("date created", None)

                    # Replace whitespaces with underscores in the keys
                    metadata = {
                        key.replace(" ", "_"): value for key, value in metadata.items()
                    }

                    # Append the metadata to the DataFrame
                    gac_df = pd.concat(
                        [gac_df, pd.DataFrame([metadata])], ignore_index=True
                    )

        else:
            print(f"Works directory not found for artist {artist_name}")

    # Serialize lists as JSON strings
    for column in gac_df.columns:
        gac_df[column] = gac_df[column].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else x
        )

    # display the resulting DataFrame
    print(gac_df)

    # Set the 'index' column as the DataFrame index
    gac_df.set_index("id", inplace=True)

    # Save the DataFrame to a CSV file
    gac_df.to_csv(csv_path)

    """
    For reading the CSV file, we need to deserialize the JSON strings back to lists:
        # Reading the CSV file and deserializing JSON strings back to lists
        # Read the CSV file
        df = pd.read_csv(csv_path, index_col='index')
    
        # Deserialize JSON strings back to lists
        for column in df.columns:
            df[column] = df[column].apply(lambda x: json.loads(x) if isinstance(x, str) and x.startswith('[') else x)
    
        # Display the resulting DataFrame
        print(df)
    """


if __name__ == "__main__":
    # Get XML path using argparse
    parser = argparse.ArgumentParser(
        description="Convert GAC metadata files to a single CSV"
    )
    parser.add_argument(
        "--root",
        type=str,
        required=True,
        help="Path to the root directory containing the folders for each artist.",
    )
    parser.add_argument(
        "--csv_path", type=str, help="Path to save the resulting CSV file", default=None
    )

    args = parser.parse_args()

    main(args.root, args.csv_path)
