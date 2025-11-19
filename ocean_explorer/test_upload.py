#!/usr/bin/env python3
"""
Test script to diagnose DigitalOcean Spaces upload issues
"""

import tempfile
from pathlib import Path
from connect_spaces import DigitalOceanSpaces

# DigitalOcean Spaces configuration
SPACES_ACCESS_KEY = "DO00CDX8Z7BFTQJ9W2AZ"
SPACES_SECRET_KEY = "kR159s1x7Xjfd6RhVIyw8X34UGLIFKSzP7/0fG6Yt9I"
SPACES_ENDPOINT = "https://blr1.digitaloceanspaces.com"
SPACES_REGION = "blr1"
SPACES_BUCKET = "historical-db-1min"
OUTPUT_PREFIX = "nautilus_main/"

def test_upload():
    """Test uploading a file to DigitalOcean Spaces"""
    print("="*80)
    print("TESTING DIGITALOCEAN SPACES UPLOAD")
    print("="*80)
    
    # Initialize Spaces connection
    print("\n1. Connecting to DigitalOcean Spaces...")
    spaces = DigitalOceanSpaces(SPACES_ACCESS_KEY, SPACES_SECRET_KEY, SPACES_ENDPOINT, SPACES_REGION)
    
    if not spaces.test_connection():
        print("[ERROR] Connection failed!")
        return False
    
    print("[OK] Connection successful")
    
    # Check if bucket exists
    print(f"\n2. Checking bucket '{SPACES_BUCKET}'...")
    try:
        buckets = spaces.list_buckets()
        bucket_names = [b['Name'] for b in buckets] if isinstance(buckets, list) else []
        if SPACES_BUCKET in bucket_names:
            print(f"[OK] Bucket '{SPACES_BUCKET}' exists")
        else:
            print(f"[WARNING] Bucket '{SPACES_BUCKET}' not found in list: {bucket_names}")
            print("  (This might be okay - bucket might still be accessible)")
    except Exception as e:
        print(f"[WARNING] Could not list buckets: {e}")
    
    # List existing files in prefix
    print(f"\n3. Checking existing files in '{OUTPUT_PREFIX}'...")
    try:
        objects = spaces.list_objects(SPACES_BUCKET, prefix=OUTPUT_PREFIX, max_keys=10, silent=True)
        if objects:
            print(f"[OK] Found {len(objects)} existing files in '{OUTPUT_PREFIX}'")
            print("  First 5 files:")
            for i, obj in enumerate(objects[:5]):
                if isinstance(obj, dict):
                    key = obj.get('Key', 'Unknown')
                    size = obj.get('Size', 0)
                    print(f"    {i+1}. {key} ({size:,} bytes)")
        else:
            print(f"[WARNING] No files found in '{OUTPUT_PREFIX}' (this is expected if no data uploaded yet)")
    except Exception as e:
        print(f"[ERROR] Error listing objects: {e}")
        import traceback
        traceback.print_exc()
    
    # Create a test file
    print(f"\n4. Creating test file...")
    test_content = b"This is a test file to verify upload functionality.\nCreated by test_upload.py"
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as test_file:
        test_file.write(test_content)
        test_file_path = test_file.name
    
    test_file_size = len(test_content)
    print(f"[OK] Created test file: {test_file_path} ({test_file_size} bytes)")
    
    # Upload test file
    test_key = f"{OUTPUT_PREFIX}test_upload_{Path(test_file_path).name}"
    print(f"\n5. Uploading test file to '{test_key}'...")
    try:
        success = spaces.upload_file(test_file_path, SPACES_BUCKET, test_key, silent=False)
        if success:
            print(f"[OK] Upload successful!")
        else:
            print(f"[ERROR] Upload returned False")
            return False
    except Exception as e:
        print(f"[ERROR] Upload failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Verify upload by checking if file exists
    print(f"\n6. Verifying upload...")
    try:
        # Try to get object metadata
        try:
            metadata = spaces.get_object_metadata(SPACES_BUCKET, test_key)
            if metadata:
                print(f"[OK] File verified in Spaces!")
                print(f"  Key: {test_key}")
                print(f"  Size: {metadata.get('ContentLength', 0):,} bytes")
            else:
                print(f"[WARNING] Could not get metadata for uploaded file")
        except Exception as e:
            print(f"[WARNING] Could not verify file metadata: {e}")
        
        # List objects again to see if test file appears
        objects = spaces.list_objects(SPACES_BUCKET, prefix=OUTPUT_PREFIX, max_keys=20, silent=True)
        test_file_found = False
        if objects:
            for obj in objects:
                if isinstance(obj, dict):
                    key = obj.get('Key', '')
                    if test_key in key or 'test_upload' in key:
                        test_file_found = True
                        print(f"[OK] Test file found in listing: {key}")
                        break
        
        if not test_file_found:
            print(f"[WARNING] Test file not found in listing (but upload may have succeeded)")
    except Exception as e:
        print(f"[WARNING] Error verifying upload: {e}")
    
    # Cleanup test file locally
    try:
        Path(test_file_path).unlink()
        print(f"\n7. Cleaned up local test file")
    except Exception as e:
        print(f"[WARNING] Could not delete local test file: {e}")
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)
    print(f"\nIf upload succeeded, you should see the test file in:")
    print(f"  Bucket: {SPACES_BUCKET}")
    print(f"  Path: {test_key}")
    print(f"\nYou can check the DigitalOcean Spaces web interface at:")
    print(f"  {SPACES_ENDPOINT.replace('https://', 'https://')}/{SPACES_BUCKET}/{OUTPUT_PREFIX}")
    print("="*80)
    
    return True


if __name__ == "__main__":
    test_upload()

