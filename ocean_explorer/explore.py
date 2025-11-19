"""
Interactive Explorer for DigitalOcean Spaces
Use this script to explore your buckets and data interactively
"""

from connect_spaces import DigitalOceanSpaces

# DigitalOcean Spaces credentials
ACCESS_KEY = "DO00CDX8Z7BFTQJ9W2AZ"
SECRET_KEY = "kR159s1x7Xjfd6RhVIyw8X34UGLIFKSzP7/0fG6Yt9I"
# Use general region endpoint for listing buckets
# The specific bucket endpoint was: https://historical-db-1min.blr1.digitaloceanspaces.com
ENDPOINT_URL = "https://blr1.digitaloceanspaces.com"
REGION = "blr1"
# Known bucket name from the original endpoint
KNOWN_BUCKET = "historical-db-1min"


def interactive_explorer():
    """Interactive explorer for DigitalOcean Spaces"""
    # Create Spaces connection
    spaces = DigitalOceanSpaces(ACCESS_KEY, SECRET_KEY, ENDPOINT_URL, REGION)
    
    print("=" * 60)
    print("DigitalOcean Spaces Explorer")
    print("=" * 60)
    
    # Test connection
    if not spaces.test_connection():
        print("\nConnection failed. Please check your credentials.")
        return
    
    # Get all buckets
    buckets = spaces.list_buckets()
    
    # If no buckets found, use the known bucket directly
    if not buckets:
        print(f"\nNo buckets found via list_buckets().")
        print(f"Using known bucket: {KNOWN_BUCKET}")
        print("=" * 60)
        # Directly explore the known bucket
        explore_bucket(spaces, KNOWN_BUCKET)
        return
    
    # Let user choose a bucket
    print("\n" + "=" * 60)
    print("Choose a bucket to explore:")
    for i, bucket in enumerate(buckets, 1):
        print(f"{i}. {bucket['Name']}")
    
    # Also show the known bucket if it's not in the list
    if KNOWN_BUCKET not in [b['Name'] for b in buckets]:
        print(f"{len(buckets) + 1}. {KNOWN_BUCKET} (known bucket)")
    
    try:
        choice = input("\nEnter bucket number (or 'all' to explore all): ").strip()
        
        if choice.lower() == 'all':
            # Explore all buckets
            for bucket in buckets:
                explore_bucket(spaces, bucket['Name'])
            # Also explore known bucket if not in list
            if KNOWN_BUCKET not in [b['Name'] for b in buckets]:
                explore_bucket(spaces, KNOWN_BUCKET)
        else:
            bucket_idx = int(choice) - 1
            total_buckets = len(buckets)
            # Check if they selected the known bucket
            if bucket_idx == total_buckets and KNOWN_BUCKET not in [b['Name'] for b in buckets]:
                explore_bucket(spaces, KNOWN_BUCKET)
            elif 0 <= bucket_idx < total_buckets:
                explore_bucket(spaces, buckets[bucket_idx]['Name'])
            else:
                print("Invalid choice.")
    except (ValueError, KeyboardInterrupt):
        print("\nExiting...")


def explore_bucket(spaces, bucket_name):
    """Explore a specific bucket"""
    print("\n" + "=" * 60)
    print(f"Exploring Bucket: {bucket_name}")
    print("=" * 60)
    
    while True:
        print("\nOptions:")
        print("1. View directory structure")
        print("2. List all objects")
        print("3. List objects with details")
        print("4. Get bucket statistics")
        print("5. Explore specific directory/prefix")
        print("6. Get metadata for specific file")
        print("7. Back to bucket selection")
        
        try:
            choice = input("\nEnter your choice (1-7): ").strip()
            
            if choice == '1':
                prefix = input("Enter directory prefix (or press Enter for root): ").strip()
                spaces.explore_directory(bucket_name, prefix=prefix)
            
            elif choice == '2':
                prefix = input("Enter prefix filter (or press Enter for all): ").strip()
                max_keys = input("Max objects to show (default 100): ").strip()
                max_keys = int(max_keys) if max_keys.isdigit() else 100
                spaces.list_objects(bucket_name, prefix=prefix, max_keys=max_keys, show_details=False)
            
            elif choice == '3':
                prefix = input("Enter prefix filter (or press Enter for all): ").strip()
                max_keys = input("Max objects to show (default 100): ").strip()
                max_keys = int(max_keys) if max_keys.isdigit() else 100
                spaces.list_objects(bucket_name, prefix=prefix, max_keys=max_keys, show_details=True)
            
            elif choice == '4':
                prefix = input("Enter prefix filter (or press Enter for all): ").strip()
                spaces.get_bucket_stats(bucket_name, prefix=prefix)
            
            elif choice == '5':
                prefix = input("Enter directory path to explore: ").strip()
                spaces.explore_directory(bucket_name, prefix=prefix)
            
            elif choice == '6':
                object_key = input("Enter object key (full path): ").strip()
                spaces.get_object_metadata(bucket_name, object_key)
            
            elif choice == '7':
                break
            
            else:
                print("Invalid choice. Please try again.")
        
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    interactive_explorer()

