import os
import xml.etree.ElementTree as ET
import ast
import re


def extract_unique_artists(xml_file):
    print("Extracting unique artists from the XML file...")

    tree = ET.parse(xml_file)
    root = tree.getroot()

    artists = set()

    for dataset in root:
        for artwork in dataset:
            artist = artwork.find("artist")
            if artist is not None and artist.text:
                artist_text = artist.text

                # Skip if the artist name is 'nan'
                if artist_text.lower() == "nan":
                    continue

                # Check if the artist text is a list representation
                if artist_text.startswith("[") and artist_text.endswith("]"):
                    try:
                        artist_list = ast.literal_eval(artist_text)
                        for artist_item in artist_list:
                            # If the list length is 1, remove descriptors
                            remove_descriptor = len(artist_list) == 1
                            artist_name = process_artist_name(
                                artist_item, remove_descriptor=remove_descriptor
                            )
                            artists.add(artist_name)
                    except (ValueError, SyntaxError):
                        # Handle cases where the string representation is not a valid list
                        artist_name = process_artist_name(artist_text)
                        artists.add(artist_name)
                else:
                    # If the text is not a list, remove descriptors
                    artist_name = process_artist_name(
                        artist_text, remove_descriptor=True
                    )
                    artists.add(artist_name)

    return list(artists)


def process_artist_name(artist_text, remove_descriptor=False):
    # Split by colon to separate descriptors from the name
    parts = artist_text.split(":")
    descriptors = ":".join(parts[:-1]).strip() if len(parts) > 1 else ""
    name_part = parts[-1].strip()

    # Keep only the name and not the info in parentheses
    artist_name = name_part.split("(")[0].strip()

    # Conditionally handle special characters if escape sequences are found
    if re.search(r"\\u[0-9a-fA-F]{4}", artist_name):
        artist_name = artist_name.encode().decode("unicode_escape")

    # If the name is written as Surname, Name then it becomes Name Surname
    if "," in artist_name:
        name_parts = artist_name.split(",")
        if len(name_parts) == 2:
            artist_name = f"{name_parts[1].strip()} {name_parts[0].strip()}"

    # Recombine descriptors and processed name if remove_descriptor is False
    if not remove_descriptor and descriptors:
        return f"{descriptors}: {artist_name}"
    else:
        return artist_name


# Example usage:
# artists = extract_unique_artists("path_to_your_file.xml")
# print(artists)


if __name__ == "__main__":
    # xml file is two directories above the current file
    xml_file_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "merged_datasets.xml"
    )
    unique_artists = extract_unique_artists(xml_file_path)
    print(f"Total unique artists: {len(unique_artists)}")

    # Save the list of artists to a text file
    output_file = os.path.join(os.path.dirname(__file__), "unique_artists.txt")
    with open(output_file, "w") as f:
        for artist in unique_artists:
            f.write(artist + "\n")
