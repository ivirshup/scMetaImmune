import requests
import json
from hashlib import md5
import requests
import pandas as pd
from argparse import ArgumentParser
from pathlib import Path

def download_file_from_google_drive(id, destination, checksum=None):
    """
    Retrieves a public file from google drive.

    If the file is too large for google's virus scanning, this will download it anyways.
    """
    def get_confirm_token(response):
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value

        return None

    def save_response_content(response, destination, checksum=None):
        CHUNK_SIZE = 32768
        if checksum:
            digest = md5()
        with open(destination, "wb") as f:
            for chunk in response.iter_content(CHUNK_SIZE):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    if checksum:
                        digest.update(chunk)
            if checksum:
                assert checksum == digest.hexdigest()

    URL = "https://drive.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params={'id': id}, stream=True)
    token = get_confirm_token(response)

    if token:
        params = { 'id' : id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)

    save_response_content(response, destination, checksum)  

def main():
    parser = ArgumentParser(description="Download bams from requested JingleBells dataset.")
    parser.add_argument("dataset", help="ID of dataset to retrieve", type=str)
    parser.add_argument("drive_table", help="TSV containing information from google drive", type=Path)
    parser.add_argument("out_dir", help="Location to download files to", type=Path)
    parser.add_argument("--dryrun", help="Should files actually be downloaded?", action="store_true")

    args = parser.parse_args()

    # Check args
    assert args.drive_table.is_file()
    table = pd.read_table(args.drive_table)

    assert (table["dataset"] == args.dataset).any(), f"Couldn't find {args.dataset} in provided drive file."

    if not args.out_dir.is_dir():
        args.out_dir.mkdir()

    # Subset table
    dset_records = table[(table["dataset"] == args.dataset) & table["name"].str.endswith(".bam")]

    for bam in dset_records.itertuples(index=False):
        bampth = args.out_dir.joinpath(bam.name)
        print(f"Downloading {bam.name} to {bampth}")
        if not args.dryrun:
            download_file_from_google_drive(bam.id, bampth, bam.md5Checksum)


if __name__ == '__main__':
    main()