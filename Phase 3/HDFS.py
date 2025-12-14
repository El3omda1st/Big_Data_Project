#!/usr/bin/env python3
# phase3_hdfs.py - Copy cleaned data from MinIO Silver to HDFS
import subprocess
import os
import time
from minio import Minio
import io

def run_hdfs_command(command):
    """Run HDFS command via Docker exec"""
    cmd = f"sudo docker exec hadoop_namenode {command}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.returncode == 0, result.stdout.strip(), result.stderr.strip()

def check_hdfs_status():
    """Check if HDFS is running"""
    print("Checking HDFS status...")
    
    # Check NameNode
    success, stdout, stderr = run_hdfs_command("hdfs dfsadmin -report 2>/dev/null | head -20")
    if success:
        print("HDFS is running")
        return True
    else:
        print("HDFS may not be running")
        print(f"Error: {stderr}")
        return False

def create_hdfs_directories():
    """Create HDFS directory structure"""
    print("\nCreating HDFS directories...")
    
    directories = [
        "/bigdata",
        "/bigdata/weather",
        "/bigdata/traffic",
        "/bigdata/merged"
    ]
    
    for directory in directories:
        success, stdout, stderr = run_hdfs_command(f"hdfs dfs -mkdir -p {directory}")
        if success:
            print(f"Created: {directory}")
        else:
            # Directory might already exist
            if "File exists" in stderr:
                print(f"Directory exists: {directory}")
            else:
                print(f"Failed to create {directory}: {stderr}")
    
    # List created directories
    print("\nHDFS directory structure:")
    run_hdfs_command("hdfs dfs -ls -R /bigdata")

def download_from_minio():
    """Download Parquet files from MinIO Silver to local"""
    print("\nDownloading files from MinIO Silver...")
    
    # Create local directory
    os.makedirs('hdfs_upload', exist_ok=True)
    
    # Connect to MinIO
    client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )
    
    files_to_download = [
        ("weather_cleaned.parquet", "hdfs_upload/weather.parquet"),
        ("traffic_cleaned.parquet", "hdfs_upload/traffic.parquet")
    ]
    
    downloaded_files = []
    
    for minio_file, local_file in files_to_download:
        try:
            # Download from MinIO
            response = client.get_object("silver", minio_file)
            with open(local_file, 'wb') as file:
                file.write(response.read())
            
            file_size = os.path.getsize(local_file)
            print(f"Downloaded: {minio_file} -> {local_file} ({file_size:,} bytes)")
            downloaded_files.append(local_file)
            
        except Exception as e:
            print(f"Failed to download {minio_file}: {e}")
    
    return downloaded_files

def upload_to_hdfs(local_files):
    """Upload local files to HDFS"""
    print("\nUploading files to HDFS...")
    
    upload_mapping = [
        ("hdfs_upload/weather.parquet", "/bigdata/weather/weather_data.parquet"),
        ("hdfs_upload/traffic.parquet", "/bigdata/traffic/traffic_data.parquet")
    ]
    
    for local_file, hdfs_path in upload_mapping:
        if os.path.exists(local_file):
            # Copy file to Docker container first
            container_path = f"/tmp/{os.path.basename(local_file)}"
            
            # Copy to container
            copy_cmd = f"sudo docker cp {local_file} hadoop_namenode:{container_path}"
            copy_result = subprocess.run(copy_cmd, shell=True, capture_output=True, text=True)
            
            if copy_result.returncode == 0:
                # Upload from container to HDFS
                hdfs_cmd = f"hdfs dfs -put {container_path} {hdfs_path}"
                success, stdout, stderr = run_hdfs_command(hdfs_cmd)
                
                if success:
                    # Get file info from HDFS
                    info_cmd = f"hdfs dfs -ls {hdfs_path}"
                    _, info_out, _ = run_hdfs_command(info_cmd)
                    print(f"Uploaded to HDFS: {hdfs_path}")
                    
                    # Show file size
                    if info_out:
                        parts = info_out.split()
                        if len(parts) >= 5:
                            size = parts[4]
                            print(f"  Size: {size} bytes")
                else:
                    print(f"Failed to upload {local_file}: {stderr}")
            else:
                print(f"Failed to copy {local_file} to container: {copy_result.stderr}")
        else:
            print(f"Local file not found: {local_file}")

def verify_hdfs_files():
    """Verify files were uploaded to HDFS"""
    print("\nVerifying HDFS files...")
    
    files_to_check = [
        "/bigdata/weather/weather_data.parquet",
        "/bigdata/traffic/traffic_data.parquet"
    ]
    
    all_good = True
    
    for hdfs_file in files_to_check:
        success, stdout, stderr = run_hdfs_command(f"hdfs dfs -test -e {hdfs_file}")
        if success:
            # Get file details
            success2, details, _ = run_hdfs_command(f"hdfs dfs -ls {hdfs_file}")
            if success2 and details:
                parts = details.split()
                if len(parts) >= 8:
                    size = parts[4]
                    date = f"{parts[5]} {parts[6]} {parts[7]}"
                    print(f"✓ {hdfs_file}")
                    print(f"  Size: {size} bytes, Modified: {date}")
        else:
            print(f"✗ Missing: {hdfs_file}")
            all_good = False
    
    return all_good

def show_hdfs_structure():
    """Display HDFS directory structure"""
    print("\n" + "=" * 60)
    print("HDFS DIRECTORY STRUCTURE")
    print("=" * 60)
    
    # Show root of our project
    run_hdfs_command("hdfs dfs -ls /")
    
    print("\n/bigdata directory:")
    run_hdfs_command("hdfs dfs -ls /bigdata")
    
    print("\n/bigdata/weather directory:")
    run_hdfs_command("hdfs dfs -ls /bigdata/weather")
    
    print("\n/bigdata/traffic directory:")
    run_hdfs_command("hdfs dfs -ls /bigdata/traffic")
    
    print("=" * 60)

def check_hdfs_health():
    """Check HDFS health and storage"""
    print("\nHDFS Health Check:")
    
    # Check disk usage
    print("\nHDFS Disk Usage:")
    run_hdfs_command("hdfs dfs -df -h")
    
    # Check DataNode status
    print("\nDataNode Status:")
    run_hdfs_command("hdfs dfsadmin -report | grep -A 5 'Datanodes available:'")
    
    # Check replication factor
    print("\nChecking replication settings...")
    run_hdfs_command("hdfs dfs -stat %r /bigdata/weather/weather_data.parquet 2>/dev/null || echo 'Cannot get replication'")

def main():
    print("PHASE 3: HDFS INTEGRATION")
    print("=" * 60)
    print("Copying cleaned data from MinIO Silver to HDFS")
    print("=" * 60)
    
    # Step 1: Check HDFS status
    if not check_hdfs_status():
        print("\nStarting HDFS services...")
        # Try to start HDFS via docker-compose
        start_cmd = "cd /home/el3omda/Big\ Data && sudo docker-compose restart namenode datanode"
        result = subprocess.run(start_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print("Failed to start HDFS. Check docker-compose.")
            return
        
        print("Waiting 30 seconds for HDFS to start...")
        time.sleep(30)
        
        if not check_hdfs_status():
            print("HDFS still not running. Check logs.")
            return
    
    # Step 2: Create HDFS directories
    create_hdfs_directories()
    
    # Step 3: Download from MinIO Silver
    local_files = download_from_minio()
    
    if not local_files:
        print("No files downloaded from MinIO. Check Silver bucket.")
        print("Run: sudo docker exec datalake_minio_setup mc ls local/silver/")
        return
    
    # Step 4: Upload to HDFS
    upload_to_hdfs(local_files)
    
    # Step 5: Verify upload
    if verify_hdfs_files():
        print("\n✓ All files successfully uploaded to HDFS")
    else:
        print("\n✗ Some files may be missing from HDFS")
    
    # Step 6: Show HDFS structure
    show_hdfs_structure()
    
    # Step 7: Check HDFS health
    check_hdfs_health()
    
    # Step 8: Cleanup local files
    print("\nCleaning up local temporary files...")
    for file in ['hdfs_upload/weather.parquet', 'hdfs_upload/traffic.parquet']:
        if os.path.exists(file):
            os.remove(file)
            print(f"Removed: {file}")
    
    # Optional: Remove directory if empty
    if os.path.exists('hdfs_upload') and not os.listdir('hdfs_upload'):
        os.rmdir('hdfs_upload')
    
    print("\n" + "=" * 60)
    print("PHASE 3 COMPLETE")
    print("=" * 60)
    print("\nFiles in HDFS:")
    print("  - /bigdata/weather/weather_data.parquet")
    print("  - /bigdata/traffic/traffic_data.parquet")
    
    print("\nTo view in HDFS Web UI:")
    print("  URL: http://localhost:9870")
    print("  Navigate to: Utilities -> Browse the file system")
    print("  Path: /bigdata")
    
    print("\nTo verify via command line:")
    print("  sudo docker exec hadoop_namenode hdfs dfs -ls -R /bigdata")
    print("=" * 60)

if __name__ == "__main__":
    main()