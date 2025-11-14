import boto3
import os

AWS_ACCESS_KEY_ID = "<YOUR_KEY>"
AWS_SECRET_ACCESS_KEY = "<YOUR_SECRET>"
ENDPOINT_URL = "https://s3.wasabisys.com"

BUCKET_NAME = "xtech"
PREFIX = "flow/us-eqt/indigo-panther/"
LOCAL_DIR = "./your_local_path"  # must be a folder

s3 = boto3.client('s3',
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                  endpoint_url=ENDPOINT_URL)

os.makedirs(LOCAL_DIR, exist_ok=True)

for obj in s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX).get('Contents', []):
    key = obj['Key']
    filename = os.path.basename(key)
    if not filename:  # skip folder keys
        continue
    local_file = os.path.join(LOCAL_DIR, filename)
    s3.download_file(BUCKET_NAME, key, local_file)
    print(f"Downloaded {key} -> {local_file}")
