#!/usr/bin/env python3
"""
Upload a file to Cloudflare R2 storage.

Usage:
    python upload_to_cloudflare.py <file_path> <cloudflare_destination>

Example:
    python upload_to_cloudflare.py ./my_file.txt folder/my_file.txt

Environment variables required:
    CLOUDFLARE_ACCOUNT_ID: Your Cloudflare account ID
    CLOUDFLARE_BUCKET_NAME: Your R2 bucket name
    CLOUDFLARE_ACCESS_KEY_ID: Your R2 API token access key
    CLOUDFLARE_SECRET_ACCESS_KEY: Your R2 API token secret key
"""

import sys
import os
import boto3
from pathlib import Path


def upload_file(file_path: str, cloudflare_destination: str) -> bool:
    """
    Upload a file to Cloudflare R2.
    
    Args:
        file_path: Local file path to upload
        cloudflare_destination: Destination path in Cloudflare R2
    
    Returns:
        True if successful, False otherwise
    """
    # Validate file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return False
    
    # Get credentials from environment variables
    account_id = os.getenv("CLOUDFLARE_ACCOUNT_ID")
    bucket_name = os.getenv("CLOUDFLARE_BUCKET_NAME")
    access_key_id = os.getenv("CLOUDFLARE_ACCESS_KEY_ID")
    secret_access_key = os.getenv("CLOUDFLARE_SECRET_ACCESS_KEY")
    
    # Validate credentials
    if not all([account_id, bucket_name, access_key_id, secret_access_key]):
        print("Error: Missing required environment variables:")
        print("  CLOUDFLARE_ACCOUNT_ID")
        print("  CLOUDFLARE_BUCKET_NAME")
        print("  CLOUDFLARE_ACCESS_KEY_ID")
        print("  CLOUDFLARE_SECRET_ACCESS_KEY")
        return False
    
    try:
        # Create S3 client configured for Cloudflare R2
        s3_client = boto3.client(
            "s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name="auto"
        )
        
        # Get file size for logging
        file_size = os.path.getsize(file_path)
        
        print(f"Uploading {file_path} ({file_size:,} bytes)...")
        print(f"Destination: {cloudflare_destination}")
        
        # Upload file
        s3_client.upload_file(
            file_path,
            bucket_name,
            cloudflare_destination
        )
        
        print(f"Successfully uploaded to {bucket_name}/{cloudflare_destination}")
        return True
        
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False


def main():
    """Parse arguments and upload file."""
    if len(sys.argv) != 3:
        print("Usage: python upload_to_cloudflare.py <file_path> <cloudflare_destination>")
        print("\nExample:")
        print("  python upload_to_cloudflare.py ./my_file.txt folder/my_file.txt")
        sys.exit(1)
    
    file_path = sys.argv[1]
    cloudflare_destination = sys.argv[2]
    
    success = upload_file(file_path, cloudflare_destination)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
