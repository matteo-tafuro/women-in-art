import os.path
import xml.etree.ElementTree as ET


def extract_unique_artists(xml_file):

    print("Extracting unique artists from the XML file...")

    tree = ET.parse(xml_file)
    root = tree.getroot()

    artists = set()

    for dataset in root:
        for artwork in dataset:
            artist = artwork.find("artist")
            if artist is not None and artist.text:
                artists.add(artist.text)

    return list(artists)


if __name__ == "__main__":
    # xml file is two directories above the current file
    xml_file_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "merged_datasets.xml"
    )
    unique_artists = extract_unique_artists(xml_file_path)
    print(f"Total unique artists: {len(unique_artists)}")

    # Save the list of artists to a text file
    output_file = os.path.join(os.path.dirname(__file__), "unique_artists.txt")
