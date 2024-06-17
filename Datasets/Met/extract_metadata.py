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

def get_args():
    parser = argparse.ArgumentParser(description='Fetch additional metadata from MET dataset.')

    parser.add_argument('--database', type=str)
    parser.add_argument('--outfile', type=str)
    parser.add_argument('--resume', default=-1, type=int)
 

    return parser


def get_props(met_id):
    url = 'https://www.metmuseum.org/art/collection/search/' + str(met_id)

    requests.get(url)
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')

    # Initialize an empty dictionary to store label-value pairs
    artwork_info = {}
    artwork_info['met_id'] = met_id

    try:
        description = soup.find('div', {'class': 'artwork__intro__desc js-artwork__intro__desc'}).text
    except:
        description = ''
        print(f'no description for: {met_id}')

    # remove all \n from description
    description = re.sub(r'\n', '', description)
    artwork_info['description'] = description
    
    # Find all "artwork-tombstone--item" elements
    try:
        details = soup.find('div', {'class': 'show-more__body js-show-more__body'})
        artwork_items = details.find_all('p', class_='artwork-tombstone--item')

    except: 
        artwork_items = []
        print(f'no details for: {met_id}')

    # Loop through each artwork item
    for item in artwork_items:
        # Find the label and value spans within the item
        label_span = item.find('span', class_='artwork-tombstone--label')
        value_span = item.find('span', class_='artwork-tombstone--value')

        try:
            # Extract text from label and value spans
            label = label_span.text.strip()[:-1]
        except:
            continue

        if label == 'Classifications':
            label = 'Classification'

        value = value_span.text.strip()

        # Add the label-value pair to the dictionary

        # make label lowercase and replace spaces with underscores
        label = re.sub(r'\s', '_', label)
        label = label.lower()

        artwork_info[label] = value

    try: 
        keywords = soup.find('meta', {"name": "keywords"})['content']
    except:
        keywords = ''
        print(f'no keywords for: {met_id}')
        
    artwork_info['keywords'] = keywords
    
    sleep(random.uniform(0, 2))

    return artwork_info


def fetch_dataset(database, outfile, resume=-1):
    # Init empty dataframe
    new_data = pd.DataFrame()

    filelist = sorted([entry['id'] for entry in json.load(open(database))])

    if resume > 0:
        res_index = filelist.index(resume) + 1
        filelist = filelist[res_index:]
    
    for i, artwork in enumerate(tqdm(filelist)):
        # intermediate save
        if i % 5 == 0:
            save_data(new_data, outfile, artwork)

        props = get_props(artwork)
        
        # concat props dictionary to new_data
        new_data = pd.concat([new_data, pd.DataFrame(props, index=[0])], ignore_index=True)

    print(f'fetched data until id: {artwork}')

    return new_data


def save_data(data, outfile, met_id=-1):
    data.to_csv(outfile, index=False)
    print(f'data saved until id: {met_id}')


if __name__ == "__main__":
    parser = get_args()
    args = parser.parse_args()


    # data_path = 'MET/'
    # database = 'ground_truth/mini_MET_database.json'

    new_data = fetch_dataset(args.database, args.outfile, args.resume)

    save_data(new_data, args.outfile)
