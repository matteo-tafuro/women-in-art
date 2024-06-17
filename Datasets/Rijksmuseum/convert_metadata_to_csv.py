import argparse
import xml.etree.ElementTree as ET
import pandas as pd
import os
import json
from tqdm import tqdm


def main(xml_path, csv_path=None):

    if csv_path is None:
        output_dir = os.path.join(os.path.dirname(os.path.dirname(xml_path)), "output")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        csv_path = os.path.join(output_dir, "rijksmuseum_metadata_final.csv")

    namespaces = {
        "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
        "dc": "http://purl.org/dc/elements/1.1/",
    }

    xml_files = [file for file in os.listdir(xml_path) if file.endswith(".xml")]

    print(f"Found {len(xml_files)} XML files in the directory! Extracting metadata...")

    # Initialize an empty DataFrame
    rijksmuseum_df = pd.DataFrame()

    for xml_file in tqdm(xml_files, desc="Processing XML files"):
        xml_file_path = os.path.join(xml_path, xml_file)

        # Read as text
        with open(xml_file_path, "r") as file:
            xml_text = file.read()

        # Extract index from filename
        index = int(xml_file.split("_")[0])

        # Corresponding image filename
        jpg_file = xml_file.replace(".xml", ".jpg")

        # Add namespaces to the XML text
        xml_text = xml_text.replace(
            "<record>",
            "<record "
            'xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" '
            'xmlns:dc="http://purl.org/dc/elements/1.1/">',
        )

        # Parse the XML text
        root = ET.fromstring(xml_text)

        # Find the 'metadata' section using XPath and namespaces
        metadata = root.find(".//oai_dc:dc", namespaces=namespaces)

        # Initialize a dictionary to store the metadata for the current file
        file_metadata = {"index": index, "filename": jpg_file}

        # Extract and store the desired information
        if metadata is not None:
            for element in metadata:
                tag = element.tag.split("}")[1]  # Strip namespace
                text = element.text

                if tag in file_metadata:
                    # If tag already exists, convert to list and append the new value
                    if isinstance(file_metadata[tag], list):
                        file_metadata[tag].append(text)
                    else:
                        file_metadata[tag] = [file_metadata[tag], text]
                else:
                    file_metadata[tag] = text

        # Append the file metadata to the DataFrame
        rijksmuseum_df = pd.concat(
            [rijksmuseum_df, pd.DataFrame([file_metadata])], ignore_index=True
        )

    # Serialize lists as JSON strings
    for column in rijksmuseum_df.columns:
        rijksmuseum_df[column] = rijksmuseum_df[column].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else x
        )

    # Set the 'index' column as the DataFrame index
    rijksmuseum_df.set_index("index", inplace=True)

    # Save the DataFrame to a CSV file
    rijksmuseum_df.to_csv(csv_path)

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
        description="Convert Rijksmuseum metadata XML files to CSV"
    )
    parser.add_argument(
        "--xml_path",
        type=str,
        required=True,
        help="Path to the directory containing the Rijksmuseum metadata XML files",
    )
    parser.add_argument(
        "--csv_path", type=str, help="Path to save the resulting CSV file", default=None
    )

    args = parser.parse_args()

    main(args.xml_path, args.csv_path)
