#!/usr/bin/env python3
"""
Simple script to list all filenames in DigitalOcean Spaces
Exports just the filenames to a text file and CSV
"""

import boto3
import csv
from botocore.exceptions import ClientError

# DigitalOcean Spaces configuration
DO_ENDPOINT = "https://blr1.digitaloceanspaces.com"
DO_REGION = "blr1"
SPACE_NAME = "historical-db-tick"
SPACE_PREFIX = "raw/parquet_data"

# Credentials
ACCESS_KEY = "DO00CDX8Z7BFTQJ9W2AZ"
SECRET_KEY = "kR159s1x7Xjfd6RhVIyw8X34UGLIFKSzP7/0fG6Yt9I"

def list_all_filenames():
    """List all filenames in the bucket"""
    
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        region_name=DO_REGION,
        endpoint_url=DO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    print(f"Connecting to DigitalOcean Spaces...")
    print(f"Bucket: {SPACE_NAME}")
    print(f"Prefix: {SPACE_PREFIX}")
    print("=" * 80)
    
    all_files = []
    continuation_token = None
    page_count = 0
    
    try:
        while True:
            page_count += 1
            
            # Prepare list_objects_v2 parameters
            params = {
                'Bucket': SPACE_NAME,
                'Prefix': SPACE_PREFIX
            }
            
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            
            # List objects
            response = s3_client.list_objects_v2(**params)
            
            # Process objects
            if 'Contents' in response:
                page_objects = response['Contents']
                
                for obj in page_objects:
                    full_path = obj['Key']
                    filename = full_path.split('/')[-1]  # Extract just the filename
                    size_mb = obj['Size'] / (1024 * 1024)
                    
                    all_files.append({
                        'filename': filename,
                        'full_path': full_path,
                        'size_mb': round(size_mb, 2)
                    })
                
                print(f"Page {page_count}: Found {len(page_objects)} files (Total: {len(all_files)})")
            
            # Check if there are more pages
            if response.get('IsTruncated', False):
                continuation_token = response.get('NextContinuationToken')
            else:
                break
        
        print("=" * 80)
        print(f"[COMPLETE] Total files found: {len(all_files)}")
        
        return all_files
        
    except ClientError as e:
        print(f"[ERROR] Error listing objects: {e}")
        return []

def export_to_text(files, filename="filenames_list.txt"):
    """Export filenames to a text file"""
    print(f"\nExporting to text file: {filename}")
    
    with open(filename, 'w', encoding='utf-8') as f:
        for file_data in files:
            f.write(f"{file_data['filename']}\n")
    
    print(f"[OK] Exported {len(files)} filenames to {filename}")

def export_to_csv(files, filename="filenames_list.csv"):
    """Export filenames to CSV"""
    print(f"\nExporting to CSV: {filename}")
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['filename', 'full_path', 'size_mb'])
        writer.writeheader()
        
        for file_data in files:
            writer.writerow(file_data)
    
    print(f"[OK] Exported {len(files)} files to {filename}")

def main():
    print("=" * 80)
    print("DIGITALOCEAN SPACES - FILENAME EXTRACTOR")
    print("=" * 80)
    print()
    
    # List all files
    files = list_all_filenames()
    
    if not files:
        print("[WARNING] No files found!")
        return
    
    # Export to text file (just filenames)
    export_to_text(files)
    
    # Export to CSV (with full path and size)
    export_to_csv(files)
    
    # Print sample
    print("\n" + "=" * 80)
    print("SAMPLE FILENAMES (First 20):")
    print("=" * 80)
    for i, file_data in enumerate(files[:20], 1):
        print(f"{i:4d}. {file_data['filename']}")
    
    if len(files) > 20:
        print(f"... and {len(files) - 20} more files")
    
    print("\n" + "=" * 80)
    print("[COMPLETE] Files exported successfully!")
    print(f"  - Text file: filenames_list.txt ({len(files)} filenames)")
    print(f"  - CSV file: filenames_list.csv ({len(files)} files with details)")
    print("=" * 80)

if __name__ == "__main__":
    main()

