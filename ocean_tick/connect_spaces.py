"""
DigitalOcean Spaces Connection Script
Connects to DigitalOcean Spaces using boto3 (S3-compatible API)
"""

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, NoCredentialsError


class DigitalOceanSpaces:
    """Class to handle DigitalOcean Spaces operations"""
    
    def __init__(self, access_key, secret_key, endpoint_url, region='blr1'):
        """
        Initialize DigitalOcean Spaces connection
        
        Args:
            access_key: DigitalOcean Spaces Access Key
            secret_key: DigitalOcean Spaces Secret Key
            endpoint_url: Spaces endpoint URL
            region: Region name (default: blr1)
        """
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.region = region
        
        # Create S3 client with DigitalOcean Spaces configuration
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(signature_version='s3v4')
        )
        
    def test_connection(self):
        """Test the connection to DigitalOcean Spaces"""
        try:
            # List buckets to test connection
            response = self.s3_client.list_buckets()
            print("✓ Successfully connected to DigitalOcean Spaces!")
            print(f"✓ Endpoint: {self.endpoint_url}")
            print(f"✓ Region: {self.region}")
            print(f"\nAvailable buckets:")
            for bucket in response.get('Buckets', []):
                print(f"  - {bucket['Name']} (created: {bucket['CreationDate']})")
            return True
        except NoCredentialsError:
            print("✗ Error: Credentials not found")
            return False
        except ClientError as e:
            print(f"✗ Error connecting to Spaces: {e}")
            return False
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
            return False
    
    def list_buckets(self):
        """List all available buckets with details"""
        try:
            response = self.s3_client.list_buckets()
            buckets = response.get('Buckets', [])
            print(f"\n{'='*60}")
            print(f"Available Buckets: {len(buckets)}")
            print(f"{'='*60}")
            for i, bucket in enumerate(buckets, 1):
                print(f"\n{i}. Bucket Name: {bucket['Name']}")
                print(f"   Created: {bucket['CreationDate']}")
                # Get bucket location
                try:
                    location = self.s3_client.get_bucket_location(Bucket=bucket['Name'])
                    print(f"   Location: {location.get('LocationConstraint', 'blr1')}")
                except:
                    pass
            return buckets
        except ClientError as e:
            print(f"✗ Error listing buckets: {e}")
            return []
    
    def list_objects(self, bucket_name, prefix='', max_keys=1000, show_details=False, silent=False):
        """
        List objects in a bucket with pagination support
        
        Args:
            bucket_name: Name of the bucket
            prefix: Optional prefix to filter objects
            max_keys: Maximum number of objects to return
            show_details: If True, show detailed information
            silent: If True, suppress print output (for programmatic use)
        """
        try:
            objects = []
            continuation_token = None
            total_size = 0
            
            if not silent:
                print(f"\n{'='*60}")
                print(f"Exploring bucket: {bucket_name}")
                if prefix:
                    print(f"Prefix filter: {prefix}")
                print(f"{'='*60}")
            
            while True:
                params = {
                    'Bucket': bucket_name,
                    'MaxKeys': min(max_keys, 1000),
                    'Prefix': prefix
                }
                if continuation_token:
                    params['ContinuationToken'] = continuation_token
                
                response = self.s3_client.list_objects_v2(**params)
                
                if 'Contents' in response:
                    objects.extend(response['Contents'])
                    for obj in response['Contents']:
                        total_size += obj['Size']
                        if not silent:
                            if show_details:
                                print(f"\n  📄 {obj['Key']}")
                                print(f"     Size: {self._format_size(obj['Size'])}")
                                print(f"     Modified: {obj['LastModified']}")
                                print(f"     ETag: {obj.get('ETag', 'N/A')}")
                            else:
                                size_str = self._format_size(obj['Size'])
                                print(f"  📄 {obj['Key']} ({size_str})")
                
                if not response.get('IsTruncated', False):
                    break
                    
                continuation_token = response.get('NextContinuationToken')
            
            if not silent:
                print(f"\n{'='*60}")
                print(f"Total objects: {len(objects)}")
                print(f"Total size: {self._format_size(total_size)}")
                print(f"{'='*60}")
            
            return objects
        except ClientError as e:
            print(f"✗ Error listing objects: {e}")
            return []
    
    def explore_directory(self, bucket_name, prefix='', delimiter='/'):
        """
        Explore directory structure (folders) in a bucket
        
        Args:
            bucket_name: Name of the bucket
            prefix: Directory prefix to explore
            delimiter: Directory delimiter (default: '/')
        """
        try:
            print(f"\n{'='*60}")
            print(f"Directory structure in bucket: {bucket_name}")
            if prefix:
                print(f"Current path: {prefix}")
            print(f"{'='*60}")
            
            # List common prefixes (directories)
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter=delimiter
            )
            
            # Show directories
            if 'CommonPrefixes' in response:
                print(f"\n📁 Directories ({len(response['CommonPrefixes'])}):")
                for prefix_info in response['CommonPrefixes']:
                    dir_name = prefix_info['Prefix']
                    print(f"  📁 {dir_name}")
            
            # Show files in current directory
            if 'Contents' in response:
                print(f"\n📄 Files in current directory ({len(response['Contents'])}):")
                for obj in response['Contents']:
                    # Only show files, not directories
                    if not obj['Key'].endswith('/'):
                        size_str = self._format_size(obj['Size'])
                        print(f"  📄 {obj['Key']} ({size_str})")
            
            return response
        except ClientError as e:
            print(f"✗ Error exploring directory: {e}")
            return None
    
    def get_object_metadata(self, bucket_name, object_key):
        """
        Get detailed metadata for a specific object
        
        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object
        """
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=object_key)
            
            print(f"\n{'='*60}")
            print(f"Object Metadata: {object_key}")
            print(f"{'='*60}")
            print(f"Size: {self._format_size(response.get('ContentLength', 0))}")
            print(f"Content Type: {response.get('ContentType', 'N/A')}")
            print(f"Last Modified: {response.get('LastModified', 'N/A')}")
            print(f"ETag: {response.get('ETag', 'N/A')}")
            print(f"Storage Class: {response.get('StorageClass', 'STANDARD')}")
            
            if 'Metadata' in response:
                print(f"\nCustom Metadata:")
                for key, value in response['Metadata'].items():
                    print(f"  {key}: {value}")
            
            return response
        except ClientError as e:
            print(f"✗ Error getting object metadata: {e}")
            return None
    
    def get_bucket_stats(self, bucket_name, prefix=''):
        """
        Get statistics about a bucket or prefix
        
        Args:
            bucket_name: Name of the bucket
            prefix: Optional prefix to filter
        """
        try:
            objects = []
            total_size = 0
            continuation_token = None
            
            print(f"\n{'='*60}")
            print(f"Bucket Statistics: {bucket_name}")
            if prefix:
                print(f"Prefix: {prefix}")
            print(f"{'='*60}")
            
            while True:
                params = {
                    'Bucket': bucket_name,
                    'MaxKeys': 1000,
                    'Prefix': prefix
                }
                if continuation_token:
                    params['ContinuationToken'] = continuation_token
                
                response = self.s3_client.list_objects_v2(**params)
                
                if 'Contents' in response:
                    objects.extend(response['Contents'])
                    for obj in response['Contents']:
                        total_size += obj['Size']
                
                if not response.get('IsTruncated', False):
                    break
                    
                continuation_token = response.get('NextContinuationToken')
            
            print(f"\nTotal Objects: {len(objects):,}")
            print(f"Total Size: {self._format_size(total_size)}")
            
            # File type breakdown
            file_types = {}
            for obj in objects:
                ext = obj['Key'].split('.')[-1] if '.' in obj['Key'] else 'no extension'
                file_types[ext] = file_types.get(ext, 0) + 1
            
            if file_types:
                print(f"\nFile Types:")
                for ext, count in sorted(file_types.items(), key=lambda x: x[1], reverse=True)[:10]:
                    print(f"  .{ext}: {count:,} files")
            
            return {
                'total_objects': len(objects),
                'total_size': total_size,
                'file_types': file_types
            }
        except ClientError as e:
            print(f"✗ Error getting bucket stats: {e}")
            return None
    
    def _format_size(self, size_bytes):
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
    
    def upload_file(self, local_file_path, bucket_name, object_name):
        """
        Upload a file to Spaces
        
        Args:
            local_file_path: Path to local file
            bucket_name: Name of the bucket
            object_name: Name for the object in Spaces
        """
        try:
            self.s3_client.upload_file(local_file_path, bucket_name, object_name)
            print(f"✓ Successfully uploaded {local_file_path} to {bucket_name}/{object_name}")
            return True
        except ClientError as e:
            print(f"✗ Error uploading file: {e}")
            return False
    
    def download_file(self, bucket_name, object_name, local_file_path):
        """
        Download a file from Spaces
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object in Spaces
            local_file_path: Path to save the downloaded file
        """
        try:
            self.s3_client.download_file(bucket_name, object_name, local_file_path)
            print(f"✓ Successfully downloaded {bucket_name}/{object_name} to {local_file_path}")
            return True
        except ClientError as e:
            print(f"✗ Error downloading file: {e}")
            return False
    
    def read_parquet_from_spaces(self, bucket_name, object_key):
        """
        Read a parquet file directly from Spaces without downloading
        
        Args:
            bucket_name: Name of the bucket
            object_key: Key of the object in Spaces
            
        Returns:
            pandas DataFrame or None if error
        """
        try:
            import pandas as pd
            from io import BytesIO
            
            # Get object from Spaces
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
            
            # Read into BytesIO buffer
            file_buffer = BytesIO(response['Body'].read())
            
            # Read parquet from buffer
            df = pd.read_parquet(file_buffer)
            
            return df
        except ClientError as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error reading {object_key} from Spaces: {e}")
            return None
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error parsing parquet from {object_key}: {e}")
            return None
    
    def delete_file(self, bucket_name, object_name):
        """
        Delete a file from Spaces
        
        Args:
            bucket_name: Name of the bucket
            object_name: Name of the object to delete
        """
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=object_name)
            print(f"✓ Successfully deleted {bucket_name}/{object_name}")
            return True
        except ClientError as e:
            print(f"✗ Error deleting file: {e}")
            return False


def main():
    """Main function to test the connection and explore"""
    # DigitalOcean Spaces credentials
    ACCESS_KEY = "DO00CDX8Z7BFTQJ9W2AZ"
    SECRET_KEY = "kR159s1x7Xjfd6RhVIyw8X34UGLIFKSzP7/0fG6Yt9I"
    # Use general region endpoint for listing buckets
    # The specific bucket endpoint was: https://historical-db-1min.blr1.digitaloceanspaces.com
    ENDPOINT_URL = "https://blr1.digitaloceanspaces.com"
    REGION = "blr1"
    # Known bucket name from the original endpoint
    KNOWN_BUCKET = "historical-db-1min"
    
    # Create Spaces connection
    spaces = DigitalOceanSpaces(ACCESS_KEY, SECRET_KEY, ENDPOINT_URL, REGION)
    
    # Test connection
    print("Connecting to DigitalOcean Spaces...")
    print("=" * 50)
    if spaces.test_connection():
        print("\n" + "=" * 50)
        print("Connection successful!")
        print("\nExploration Methods Available:")
        print("  - list_buckets() - List all buckets")
        print("  - list_objects(bucket_name, prefix='', show_details=False) - List objects")
        print("  - explore_directory(bucket_name, prefix='') - Browse folder structure")
        print("  - get_bucket_stats(bucket_name, prefix='') - Get statistics")
        print("  - get_object_metadata(bucket_name, object_key) - Get file details")
        print("\nFile Operations:")
        print("  - upload_file(local_path, bucket_name, object_name)")
        print("  - download_file(bucket_name, object_name, local_path)")
        print("  - delete_file(bucket_name, object_name)")
        
        # Try to list all buckets
        print("\n" + "=" * 50)
        print("Listing all buckets...")
        buckets = spaces.list_buckets()
        
        # If no buckets found, try accessing the known bucket directly
        if not buckets:
            print(f"\nNo buckets found via list_buckets().")
            print(f"Trying to access known bucket: {KNOWN_BUCKET}")
            print("=" * 50)
            try:
                # Try to explore the known bucket directly
                spaces.explore_directory(KNOWN_BUCKET)
                spaces.get_bucket_stats(KNOWN_BUCKET)
            except Exception as e:
                print(f"Could not access bucket '{KNOWN_BUCKET}': {e}")
        else:
            # If buckets exist, show example exploration
            bucket_name = buckets[0]['Name']
            print(f"\n{'='*50}")
            print(f"Example: Exploring bucket '{bucket_name}'")
            print(f"{'='*50}")
            
            # Show directory structure
            spaces.explore_directory(bucket_name)
            
            # Get bucket statistics
            spaces.get_bucket_stats(bucket_name)
    else:
        print("\nConnection failed. Please check your credentials.")


if __name__ == "__main__":
    main()

