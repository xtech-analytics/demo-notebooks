import boto3
import os
import argparse
from getpass import getpass

def main():
    # === CLI argument parser ===
    parser = argparse.ArgumentParser(description="Download files from an S3-compatible bucket (e.g., Wasabi).")
    parser.add_argument("--access-key", help="AWS Access Key ID (or set AWS_ACCESS_KEY_ID env var)")
    parser.add_argument("--secret-key", help="AWS Secret Access Key (or set AWS_SECRET_ACCESS_KEY env var)")
    parser.add_argument("--endpoint", default="https://s3.wasabisys.com", help="S3 endpoint URL")
    parser.add_argument("--bucket", required=True, help="Bucket name")
    parser.add_argument("--prefix", required=True, help="Prefix (folder path in bucket)")
    parser.add_argument("--local-dir", default="./data", help="Local directory to save files")

    args = parser.parse_args()

    # === Resolve credentials (CLI arg > env var > prompt) ===
    aws_access_key = (
        args.access_key
        or os.getenv("AWS_ACCESS_KEY_ID")
        or input("Enter AWS Access Key ID: ")
    )
    aws_secret_key = (
        args.secret_key
        or os.getenv("AWS_SECRET_ACCESS_KEY")
        or getpass("Enter AWS Secret Access Key: ")
    )

    # === Initialize S3 client ===
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        endpoint_url=args.endpoint
    )

    # === Ensure local directory exists ===
    os.makedirs(args.local_dir, exist_ok=True)

    print(f"🚀 Starting download from s3://{args.bucket}/{args.prefix}")
    print(f"📁 Files will be saved under: {os.path.abspath(args.local_dir)}\n")

    # === Paginate through files ===
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=args.bucket, Prefix=args.prefix)

    file_count = 0

    for page in page_iterator:
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            if key.endswith("/"):
                continue

            # Preserve folder structure locally
            local_path = os.path.join(args.local_dir, key)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            # Download
            try:
                s3.download_file(args.bucket, key, local_path)
                print(f"✅ Downloaded {key} -> {local_path}")
                file_count += 1
            except Exception as e:
                print(f"❌ Failed to download {key}: {e}")

    if file_count == 0:
        print("⚠️ No files found with the given prefix.")
    else:
        print(f"\n🎉 Download complete! Total files downloaded: {file_count}")

if __name__ == "__main__":
    main()
