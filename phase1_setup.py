#!/usr/bin/env python3
# upload_direct.py - Upload CSV files directly to Bronze bucket
import os
from minio import Minio

def main():
    print("📤 UPLOADING DATA TO MINIO BRONZE BUCKET")
    print("="*50)
    
    # Connect to MinIO
    client = Minio(
        "localhost:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )
    
    # Files to upload
    files = [
        ("weather_data_raw.csv", "Data Generation/weather_data_raw.csv"),
        ("traffic_data_raw.csv", "Data Generation/traffic_data_raw.csv")
    ]
    
    # Check if files exist
    for obj_name, file_path in files:
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return
    
    print("✅ Found both CSV files")
    
    # Create Bronze bucket if needed
    try:
        if not client.bucket_exists("bronze"):
            client.make_bucket("bronze")
            print("✅ Created bronze bucket")
        else:
            print("📦 Bronze bucket exists")
    except Exception as e:
        print(f"⚠️  Bucket error: {e}")
    
    # Upload files directly to bronze bucket
    print("\n📤 Uploading to bronze/...")
    for obj_name, file_path in files:
        try:
            client.fput_object(
                bucket_name="bronze",
                object_name=obj_name,  # Directly in bucket, no folder
                file_path=file_path,
                content_type="text/csv"
            )
            size = os.path.getsize(file_path)
            print(f"✅ Uploaded: {obj_name} ({size:,} bytes)")
        except Exception as e:
            print(f"❌ Failed to upload {obj_name}: {e}")
    
    # List contents
    print("\n📁 Contents of bronze bucket:")
    try:
        objects = client.list_objects("bronze")
        for obj in objects:
            print(f"   📄 {obj.object_name}")
    except:
        print("   Could not list bucket contents")
    
    print("\n" + "="*50)
    print("🎉 UPLOAD COMPLETE!")
    print("="*50)
    print("\n🔗 MinIO Console: http://localhost:9001")
    print("   Username: admin")
    print("   Password: password123")
    print("\n📊 Files uploaded:")
    print("   • weather_data_raw.csv")
    print("   • traffic_data_raw.csv")

if __name__ == "__main__":
    main()