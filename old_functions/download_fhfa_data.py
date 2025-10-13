# Import Packages
import logging
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time


logger = logging.getLogger(__name__)

# Download FHFA Data
def download_fhfa_data(base_url: str, download_dir: str, allowed_extensions: list[str], included_substring: str='', pause_length: int=5) :

    # Create the download directory if it doesn't exist
    os.makedirs(download_dir, exist_ok=True)

    # Fetch the webpage content
    response = requests.get(base_url)
    response.raise_for_status()  # Raise an exception for HTTP errors

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all anchor tags with href attributes
    links = soup.find_all('a', href=True)

    # Iterate over the links
    for link in links:

        # Get the HREF Link
        href = link['href']

        # Construct the full URL
        file_url = urljoin(base_url, href)

        # Parse the URL to get the path
        parsed_url = urlparse(file_url)

        # Extract the file name from the URL path
        file_name = os.path.basename(parsed_url.path)

        # Skip if the file name is empty
        if not file_name:
            continue

        # Check for valid file extension
        _, ext = os.path.splitext(file_name)
        if ext.lower() not in allowed_extensions:
            continue

        # Check for Included Substring
        if included_substring not in file_name:
            continue

        # Define the full path to save the file
        file_path = os.path.join(download_dir, file_name)

        # Skip if the file already exists
        if os.path.exists(file_path):
            continue

        # Download and Save the Files
        try:

            # Send a GET request to download the file
            logger.info('Downloading %s...', file_url)
            file_response = requests.get(file_url)
            file_response.raise_for_status()

            # Write the content to a file
            with open(file_path, 'wb') as f:
                f.write(file_response.content)
            logger.info('Saved to %s', file_path)

            # Pause for a while
            time.sleep(pause_length)

        except requests.RequestException as e:
            logger.error('Failed to download %s: %s', file_url, e)

## Main Routine
if __name__=='__main__':
    logging.basicConfig(level=logging.INFO)

    # URL of the FHFA Public Use Database page
    base_url = 'https://www.fhfa.gov/data/pudb'

    # Download FHFA Data Files (zip files with pudb substring)
    download_dir = './data/raw/fhfa'
    download_fhfa_data(base_url, download_dir, included_substring='pudb', allowed_extensions=['.zip'])

    # Download FHLB Data Files (csv files with pudb substring)
    download_dir = './data/raw/fhlb'
    download_fhfa_data(base_url, download_dir, included_substring='pudb', allowed_extensions=['.csv'])

    # Download Data Files
    download_dir = './dictionary_files'
    download_fhfa_data(base_url, download_dir, allowed_extensions=['.pdf'])
