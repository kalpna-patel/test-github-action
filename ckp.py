import os
import requests
import argparse
from datetime import datetime
from urllib.parse import urlparse
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from requests.exceptions import RequestException
from typing import List

def log(message: str) -> None:
    """Logs a message with a timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

def authenticate_azure(storage_account_name: str) -> BlobServiceClient:
    """Authenticates with Azure and returns a BlobServiceClient."""
    try:
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=credential
        )
        return blob_service_client
    except Exception as e:
        log(f"Failed to authenticate with Azure: {e}")
        exit(1)

def convert_github_url(url: str) -> str:
    """Converts a GitHub URL to a raw content URL if necessary."""
    parsed_url = urlparse(url)
    if "github.com" in parsed_url.netloc:
        return url.replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
    return url

def upload_blob(blob_service_client: BlobServiceClient, container_name: str, file_path: str, blob_name: str = None) -> None:
    """Uploads a file to Azure Blob Storage."""
    try:
        if not blob_name:
            blob_name = os.path.basename(file_path)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        if file_path.startswith("http://") or file_path.startswith("https://"):
            # Convert GitHub URL to raw content URL if necessary
            file_path = convert_github_url(file_path)

            # Download the file content
            response = requests.get(file_path, stream=True)
            response.raise_for_status()  # Raise exception for HTTP errors

            content_type = response.headers.get('Content-Type', '')
            if 'text/html' in content_type or not response.content:
                log(f"Invalid file content or URL points to an HTML page: {file_path}")
                return

            # Upload the file content to Azure Blob
            blob_client.upload_blob(response.content, overwrite=True)
            log(f"Uploaded remote file {file_path} to blob {blob_name}")

        else:
            # Upload the local file
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            log(f"Uploaded local file {file_path} to blob {blob_name}")

    except RequestException as req_err:
        log(f"HTTP request failed for {file_path}: {req_err}")
    except Exception as e:
        log(f"Failed to upload {file_path} to blob {blob_name}: {e}")

def process_file(blob_service_client: BlobServiceClient, container_name: str, file_path: str, blob_name: str = None) -> None:
    """Processes a file by uploading it to Azure Blob Storage."""
    try:
        upload_blob(blob_service_client, container_name, file_path, blob_name)
    except Exception as e:
        log(f"An error occurred while processing {file_path}: {e}")

def handle_input(blob_service_client: BlobServiceClient, container_name: str, input_paths: List[str]) -> None:
    """Handles input paths, determining if they are URLs or local files, and processes them accordingly."""
    for path in input_paths:
        process_file(blob_service_client, container_name, path)

def main(storage_account_name: str, container_name: str, input_paths: List[str]) -> None:
    """Main function to authenticate with Azure and handle input paths."""
    blob_service_client = authenticate_azure(storage_account_name)
    handle_input(blob_service_client, container_name, input_paths)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest data into Azure Storage Account.')
    parser.add_argument('--storage-account-name', required=True, help='Azure Storage Account name')
    parser.add_argument('--container-name', required=True, help='Azure Storage Account container name')
    parser.add_argument('--input-paths', required=True, help='Comma-separated list of local files or remote URLs to upload')
    
    args = parser.parse_args()
    input_paths = args.input_paths.split(',')
    main(args.storage_account_name, args.container_name, input_paths)

