"""
Interactive Explorer for DigitalOcean Spaces
Use this script to explore your buckets and data interactively.

Credentials and target bucket/endpoint can be provided via CLI flags or the
standard DigitalOcean environment variables (DO_ACCESS_KEY, DO_SECRET_KEY, etc).
"""

import argparse
import os

from connect_spaces import DigitalOceanSpaces

DEFAULT_ACCESS_KEY = os.environ.get("DO_ACCESS_KEY", "DO00CDX8Z7BFTQJ9W2AZ")
DEFAULT_SECRET_KEY = os.environ.get("DO_SECRET_KEY", "kR159s1x7Xjfd6RhVIyw8X34UGLIFKSzP7/0fG6Yt9I")
DEFAULT_ENDPOINT = os.environ.get("DO_ENDPOINT", "https://blr1.digitaloceanspaces.com")
DEFAULT_REGION = os.environ.get("DO_REGION", "blr1")
DEFAULT_BUCKET = os.environ.get("DO_BUCKET", "historical-db-tick")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Interactive DigitalOcean Spaces explorer"
    )
    parser.add_argument(
        "--access-key",
        default=DEFAULT_ACCESS_KEY,
        help="DigitalOcean Spaces access key (default: DO_ACCESS_KEY env or built-in demo key)"
    )
    parser.add_argument(
        "--secret-key",
        default=DEFAULT_SECRET_KEY,
        help="DigitalOcean Spaces secret key (default: DO_SECRET_KEY env or built-in demo key)"
    )
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help="DigitalOcean Spaces endpoint URL (default: DO_ENDPOINT env or blr1 endpoint)"
    )
    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help="DigitalOcean Spaces region (default: DO_REGION env or 'blr1')"
    )
    parser.add_argument(
        "--bucket",
        default=DEFAULT_BUCKET,
        help="Preferred bucket to explore when list_buckets is empty"
    )
    return parser.parse_args()


def interactive_explorer(access_key, secret_key, endpoint_url, region, known_bucket):
    """Interactive explorer for DigitalOcean Spaces"""
    # Create Spaces connection
    spaces = DigitalOceanSpaces(access_key, secret_key, endpoint_url, region)
    
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
        print("\nNo buckets found via list_buckets().")
        print(f"Using known bucket: {known_bucket}")
        print("=" * 60)
        # Directly explore the known bucket
        explore_bucket(spaces, known_bucket)
        return
    
    # Let user choose a bucket
    print("\n" + "=" * 60)
    print("Choose a bucket to explore:")
    for i, bucket in enumerate(buckets, 1):
        print(f"{i}. {bucket['Name']}")
    
    # Also show the known bucket if it's not in the list
    if known_bucket not in [b['Name'] for b in buckets]:
        print(f"{len(buckets) + 1}. {known_bucket} (known bucket)")
    
    try:
        choice = input("\nEnter bucket number (or 'all' to explore all): ").strip()
        
        if choice.lower() == 'all':
            # Explore all buckets
            for bucket in buckets:
                explore_bucket(spaces, bucket['Name'])
            # Also explore known bucket if not in list
            if known_bucket not in [b['Name'] for b in buckets]:
                explore_bucket(spaces, known_bucket)
        else:
            bucket_idx = int(choice) - 1
            total_buckets = len(buckets)
            # Check if they selected the known bucket
            if bucket_idx == total_buckets and known_bucket not in [b['Name'] for b in buckets]:
                explore_bucket(spaces, known_bucket)
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
    args = parse_args()

    if not args.access_key or not args.secret_key:
        print("Error: DigitalOcean Spaces credentials are required. Use --access-key/--secret-key or set DO_ACCESS_KEY/DO_SECRET_KEY.")
        raise SystemExit(1)

    interactive_explorer(
        access_key=args.access_key,
        secret_key=args.secret_key,
        endpoint_url=args.endpoint,
        region=args.region,
        known_bucket=args.bucket,
    )

