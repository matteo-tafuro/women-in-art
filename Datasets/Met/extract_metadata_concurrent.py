import json
import pandas as pd
import os
import re
import requests
import random
from bs4 import BeautifulSoup
from time import sleep
from tqdm import tqdm
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


def get_args():
    parser = argparse.ArgumentParser(
        description="Fetch additional metadata from MET dataset."
    )
    parser.add_argument("--database", type=str)
    parser.add_argument("--outfile", type=str)
    parser.add_argument("--resume", default=-1, type=int)
    parser.add_argument(
        "--max_workers", type=int, default=5, help="Number of parallel workers"
    )
    return parser


def get_props(met_id):
    url = "https://www.metmuseum.org/art/collection/search/" + str(met_id)
    soup = BeautifulSoup(requests.get(url).text, "html.parser")

    artwork_info = {"met_id": met_id}
    missing_info = {"no_description": [], "no_details": [], "no_keywords": []}

    try:
        description = soup.find(
            "div", {"class": "artwork__intro__desc js-artwork__intro__desc"}
        ).text
    except:
        description = ""
        missing_info["no_description"].append(met_id)
    description = re.sub(r"\n", "", description)
    artwork_info["description"] = description

    try:
        details = soup.find("div", {"class": "show-more__body js-show-more__body"})
        artwork_items = details.find_all("p", class_="artwork-tombstone--item")
    except:
        artwork_items = []
        missing_info["no_details"].append(met_id)

    for item in artwork_items:
        label_span = item.find("span", class_="artwork-tombstone--label")
        value_span = item.find("span", class_="artwork-tombstone--value")
        try:
            label = label_span.text.strip()[:-1]
        except:
            continue
        if label == "Classifications":
            label = "Classification"
        value = value_span.text.strip()
        label = re.sub(r"\s", "_", label).lower()
        artwork_info[label] = value

    try:
        keywords = soup.find("meta", {"name": "keywords"})["content"]
    except:
        keywords = ""
        missing_info["no_keywords"].append(met_id)
    artwork_info["keywords"] = keywords

    sleep(random.uniform(0, 2))
    return artwork_info, missing_info


def fetch_dataset(database, outfile, resume=-1, max_workers=5):
    filelist = sorted([entry["id"] for entry in json.load(open(database))])

    if resume > 0:
        res_index = filelist.index(resume) + 1
        filelist = filelist[res_index:]

    # Create a tmp directory for temporary files
    tmp_dir = os.path.join(os.path.dirname(outfile), "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    # Partition the filelist among workers
    partitions = [filelist[i::max_workers] for i in range(max_workers)]

    # Initialize a lock object
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        tqdm_bars = [
            tqdm(total=len(partition), desc=f"Worker {i+1}", position=i)
            for i, partition in enumerate(partitions)
        ]
        for i, partition in enumerate(partitions):
            futures.append(
                executor.submit(
                    process_partition,
                    partition,
                    f"{tmp_dir}/{os.path.basename(outfile)}.part{i}",
                    lock,
                    tqdm_bars[i],
                )
            )

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Generated an exception: {exc}")

    # Close tqdm bars
    for bar in tqdm_bars:
        bar.close()

    # Merge all parts into the final output file
    merge_files(outfile, tmp_dir, max_workers)

    # Merge missing info files
    merge_missing_info_files(outfile, tmp_dir, max_workers)

    # Remove tmp directory
    os.rmdir(tmp_dir)


def process_partition(partition, outfile, lock, progress_bar):
    new_data = pd.DataFrame()
    missing_info = {"no_description": [], "no_details": [], "no_keywords": []}

    for i, artwork in enumerate(partition):
        props, missing = get_props(artwork)
        new_data = pd.concat(
            [new_data, pd.DataFrame(props, index=[0])], ignore_index=True
        )
        for key in missing_info:
            missing_info[key].extend(missing[key])

        if i % 10 == 0:
            with lock:
                save_data(new_data, outfile)
                save_missing_info(missing_info, outfile)
                new_data = pd.DataFrame()  # Clear the dataframe after saving
                missing_info = {
                    "no_description": [],
                    "no_details": [],
                    "no_keywords": [],
                }  # Clear the missing info

        progress_bar.update(1)  # Update the progress bar

    with lock:
        save_data(new_data, outfile)
        save_missing_info(missing_info, outfile)
    progress_bar.update(
        len(partition) - progress_bar.n
    )  # Ensure the progress bar is complete


def save_data(data, outfile):
    if os.path.exists(outfile):
        existing_data = pd.read_csv(outfile)
        combined_data = pd.concat([existing_data, data], ignore_index=True).reset_index(
            drop=True
        )
        combined_data.to_csv(outfile, index=False)
    else:
        data.to_csv(outfile, index=False)


def save_missing_info(missing_info, outfile):
    for key, ids in missing_info.items():
        file_path = f"{outfile}.{key}"
        with open(file_path, "a") as f:
            for id in ids:
                f.write(f"{id}\n")


def merge_files(outfile, tmp_dir, parts):
    all_columns = set()

    # Collect all columns from all parts
    for i in range(parts):
        part_file = f"{tmp_dir}/{os.path.basename(outfile)}.part{i}"
        if os.path.exists(part_file):
            df = pd.read_csv(part_file)
            all_columns.update(df.columns)

    all_columns = sorted(list(all_columns))

    with open(outfile, "w") as outfile_handle:
        header_written = False
        for i in range(parts):
            part_file = f"{tmp_dir}/{os.path.basename(outfile)}.part{i}"
            if os.path.exists(part_file):
                df = pd.read_csv(part_file)
                df = df.reindex(columns=all_columns)
                if not header_written:
                    df.to_csv(outfile_handle, index=False)
                    header_written = True
                else:
                    df.to_csv(outfile_handle, index=False, header=False)
                os.remove(part_file)  # Clean up part file


def merge_missing_info_files(outfile, tmp_dir, parts):
    for key in ["no_description", "no_details", "no_keywords"]:
        merged_file = f"{outfile}.{key}"
        with open(merged_file, "w") as outfile_handle:
            for i in range(parts):
                part_file = f"{tmp_dir}/{os.path.basename(outfile)}.part{i}.{key}"
                if os.path.exists(part_file):
                    with open(part_file, "r") as part_handle:
                        while True:
                            line = part_handle.readline()
                            if not line:
                                break
                            outfile_handle.write(line)
                    os.remove(part_file)  # Clean up part file


if __name__ == "__main__":
    parser = get_args()
    args = parser.parse_args()

    fetch_dataset(args.database, args.outfile, args.resume, args.max_workers)
