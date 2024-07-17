import os
import pandas as pd
from tqdm import tqdm
from xml.sax.saxutils import escape
import re


def clean_text(text):
    """Remove non-printable characters from text."""
    if isinstance(text, str):
        # Remove non-printable characters
        text = re.sub(r"[\x00-\x1F\x7F-\x9F]", "", text)
    return text


def main(root_dir=None, output_file=None):
    if not root_dir:
        root_dir = os.getcwd()
        print(f"Root directory not provided. Using default: {root_dir}")
    if not output_file:
        output_file = os.path.join(os.getcwd(), "merged_datasets.xml")

    # Step 1: Read CSV files into DataFrames
    print("Reading CSV files into DataFrames...")
    with tqdm(total=6, desc="Reading CSV files") as bar:
        met_df = pd.read_csv(
            os.path.join(root_dir, "Met/output/met_metadata_final.csv")
        )
        bar.update(1)

        semart_df = pd.read_csv(
            os.path.join(root_dir, "SemArt/output/semart_metadata_final.csv")
        )
        bar.update(1)

        rijksmuseum_df = pd.read_csv(
            os.path.join(root_dir, "Rijksmuseum/output/rijksmuseum_metadata_final.csv")
        )
        bar.update(1)

        ukiyoe_df = pd.read_csv("Ukiyo-e/output/ukiyoe_metadata_final.csv", index_col=0)
        bar.update(1)

        wikiart_df = pd.read_csv(
            os.path.join(root_dir, "Wikiart/output/wikiart_metadata_final.csv")
        )
        bar.update(1)

        gac_df = pd.read_csv(
            os.path.join(root_dir, "GAC/output/gac_metadata_final.csv")
        )
        bar.update(1)

    # Step 2: Clean and rename overlapping fields
    print("Cleaning and renaming overlapping fields...")

    # Replace slashes with underscores in MET DataFrame columns
    met_df.columns = met_df.columns.str.replace("/", "_")
    met_df = met_df.apply(lambda col: col.apply(clean_text))

    met_df.rename(
        columns={
            "description": "description",
            "artist": "artist",
            "title": "title",
            "date": "date",
            "medium": "technique",
            "type": "type",
        },
        inplace=True,
    )
    semart_df = semart_df.apply(lambda col: col.apply(clean_text))
    semart_df.rename(
        columns={
            "IMAGE_FILE": "image_file",
            "DESCRIPTION": "description",
            "AUTHOR": "artist",
            "TITLE": "title",
            "TECHNIQUE": "technique",
            "DATE": "date",
            "TYPE": "type",
            "SCHOOL": "school",
            "SPLIT": "split",
            "TIMEFRAME": "timeframe",
        },
        inplace=True,
    )
    rijksmuseum_df = rijksmuseum_df.apply(lambda col: col.apply(clean_text))
    rijksmuseum_df.rename(
        columns={
            "filename": "image_file",
            "description": "description",
            "creator": "artist",
            "title": "title",
            "date": "date",
            "type": "type",
        },
        inplace=True,
    )
    ukiyoe_df = ukiyoe_df.apply(lambda col: col.apply(clean_text))
    ukiyoe_df.rename(
        columns={
            "image_file": "image_file",
            "description": "description",
            "artistString": "artist",
            "title": "title",
            "date": "date",
            "type": "type",
        },
        inplace=True,
    )
    # Drop the 'Unnamed: 42' column
    ukiyoe_df.drop("Unnamed: 42", axis=1, inplace=True)
    wikiart_df = wikiart_df.apply(lambda col: col.apply(clean_text))
    wikiart_df.rename(
        columns={
            "description": "description",
            "filename": "image_file",
            "artist": "artist",
            "title": "title",
            "date": "date",
            "genre": "type",
        },
        inplace=True,
    )
    gac_df = gac_df.apply(lambda col: col.apply(clean_text))
    gac_df.rename(
        columns={
            "artwork_path": "image_file",
            "main_text": "description",
            "creator": "artist",
            "title": "title",
            "date": "date",
            "type": "type",
        },
        inplace=True,
    )

    # Step 3: Convert each DataFrame to XML and write incrementally
    print("Writing DataFrames to XML incrementally...")
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("<Datasets>\n")

        with tqdm(total=6, desc="Converting DataFrames to XML") as pbar:
            write_dataframe_to_xml(f, met_df, "Met", "artwork")
            pbar.update(1)

            write_dataframe_to_xml(f, semart_df, "SemArt", "artwork")
            pbar.update(1)

            write_dataframe_to_xml(f, rijksmuseum_df, "Rijksmuseum", "artwork")
            pbar.update(1)

            write_dataframe_to_xml(f, ukiyoe_df, "Ukiyo-e", "artwork")
            pbar.update(1)

            write_dataframe_to_xml(f, wikiart_df, "WikiArt", "artwork")
            pbar.update(1)

            write_dataframe_to_xml(f, gac_df, "GAC", "artwork")
            pbar.update(1)

        f.write("</Datasets>\n")

    print(f"Merged XML saved as '{output_file}'.")


def write_dataframe_to_xml(file_handle, df, root_name, row_name):
    """Write a DataFrame to an XML file incrementally."""
    file_handle.write(f"  <{root_name}>\n")
    for i, row in df.iterrows():
        file_handle.write(f"    <{row_name}>\n")
        for field, value in row.items():
            file_handle.write(
                f"      <{field}>{escape(clean_text(str(value)))}</{field}>\n"
            )
        file_handle.write(f"    </{row_name}>\n")
    file_handle.write(f"  </{root_name}>\n")


if __name__ == "__main__":
    main()
