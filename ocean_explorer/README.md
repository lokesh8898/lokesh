# DigitalOcean Spaces Connection & Explorer

This project provides Python scripts to connect to and interact with DigitalOcean Spaces (S3-compatible object storage), with powerful exploration capabilities.

## Setup

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Quick Start - Interactive Explorer

Run the interactive explorer to browse your buckets and data:
```bash
python explore.py
```

This will:
- Connect to your DigitalOcean Spaces
- List all available buckets
- Let you choose a bucket to explore
- Provide interactive menus to browse directories, view files, and get statistics

### Basic Connection Test

Run the main script to test the connection:
```bash
python connect_spaces.py
```

### Using the DigitalOceanSpaces Class

```python
from connect_spaces import DigitalOceanSpaces

# Initialize connection
spaces = DigitalOceanSpaces(
    access_key="DO00CDX8Z7BFTQJ9W2AZ",
    secret_key="kR159s1x7Xjfd6RhVIyw8X34UGLIFKSzP7/0fG6Yt9I",
    endpoint_url="https://historical-db-1min.blr1.digitaloceanspaces.com",
    region="blr1"
)

# Test connection
spaces.test_connection()

# ===== EXPLORATION METHODS =====

# List all buckets with details
spaces.list_buckets()

# List objects in a bucket (with pagination support)
spaces.list_objects("bucket-name")
spaces.list_objects("bucket-name", prefix="folder/", show_details=True)

# Explore directory structure (browse folders)
spaces.explore_directory("bucket-name")
spaces.explore_directory("bucket-name", prefix="folder/subfolder/")

# Get bucket statistics (total objects, size, file types)
spaces.get_bucket_stats("bucket-name")
spaces.get_bucket_stats("bucket-name", prefix="folder/")

# Get detailed metadata for a specific file
spaces.get_object_metadata("bucket-name", "path/to/file.txt")

# ===== FILE OPERATIONS =====

# Upload a file
spaces.upload_file("local_file.txt", "bucket-name", "remote_file.txt")

# Download a file
spaces.download_file("bucket-name", "remote_file.txt", "downloaded_file.txt")

# Delete a file
spaces.delete_file("bucket-name", "remote_file.txt")
```

## Features

### Exploration Features
- ✅ **List all buckets** - View all available buckets with creation dates
- ✅ **Browse directory structure** - Navigate folders like a file explorer
- ✅ **List objects** - View files with pagination support for large buckets
- ✅ **Detailed object view** - See file sizes, modification dates, and metadata
- ✅ **Bucket statistics** - Get total objects, sizes, and file type breakdowns
- ✅ **Object metadata** - View detailed information about specific files

### File Operations
- ✅ Upload files to Spaces
- ✅ Download files from Spaces
- ✅ Delete files from Spaces

### Additional Features
- ✅ Human-readable file sizes (KB, MB, GB, TB)
- ✅ Pagination support for large buckets
- ✅ Prefix filtering for exploring specific directories
- ✅ Error handling and connection testing

## Configuration

The credentials are currently hardcoded in the scripts. For production use, consider:
- Using environment variables
- Using a configuration file
- Using a secrets management service

## Examples

### Example 1: Explore a specific folder
```python
spaces.explore_directory("my-bucket", prefix="data/2024/")
```

### Example 2: Get statistics for a prefix
```python
stats = spaces.get_bucket_stats("my-bucket", prefix="logs/")
# Returns: total objects, total size, file type breakdown
```

### Example 3: List files with details
```python
objects = spaces.list_objects("my-bucket", prefix="documents/", show_details=True)
```

### Example 4: Get file metadata
```python
metadata = spaces.get_object_metadata("my-bucket", "path/to/important-file.csv")
```

