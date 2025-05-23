# Import Packages
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time

# Download FHFA Data
def download_fhfa_data(base_url:str, download_dir:str, pause_length: int=5) :

    # Create the download directory if it doesn't exist
    os.makedirs(download_dir, exist_ok=True)

    # Allowed file extensions
    allowed_extensions = {'.zip', '.txt', '.csv', '.pdf'}

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

        # Define the full path to save the file
        file_path = os.path.join(download_dir, file_name)

        # Download and Save the Files
        try:

            # Send a GET request to download the file
            print(f'Downloading {file_url}...')
            file_response = requests.get(file_url)
            file_response.raise_for_status()

            # Write the content to a file
            with open(file_path, 'wb') as f:
                f.write(file_response.content)
            print(f'Saved to {file_path}')

            # Pause for a while
            time.sleep(pause_length)

        except requests.RequestException as e:
            print(f'Failed to download {file_url}: {e}')

## Main Routine
if __name__=='__main__':

    # URL of the FHFA Public Use Database page
    base_url = 'https://www.fhfa.gov/data/pudb'

    # Directory to save downloaded files
    download_dir = './data/raw'

    # Download Data
    download_fhfa_data(base_url, download_dir)
    
