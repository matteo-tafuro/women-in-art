import os
import pandas as pd


def merge_metadata_files(metadata_directory, output_file="met_metadata_final.csv"):

    # Get all the csv files in the directory
    file_names = [
        file for file in os.listdir(metadata_directory) if file.endswith(".csv")
    ]

    print(f"Found {len(file_names)} CSV files!")

    # Read and concatenate the DataFrames
    dataframes = [
        pd.read_csv(os.path.join(metadata_directory, file)) for file in file_names
    ]

    # Merge all DataFrames
    merged_df = pd.concat(dataframes, axis=0, ignore_index=True)

    # Save the merged DataFrame to a new CSV file
    merged_df.to_csv(output_file, index=False)

    print("Merged CSV file saved as 'merged_metadata.csv'")


if __name__ == "__main__":
    # parent directory of the current file
    current_directory = os.path.dirname(__file__)
    merge_metadata_files(os.path.join(current_directory, "data/MET metadata first 6"))
