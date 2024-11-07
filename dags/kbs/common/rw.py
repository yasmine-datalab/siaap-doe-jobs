import logging
from typing import List
import json
import io
from datetime import datetime
from minio import Minio
import redis
from redis.commands.json import path
from pathlib import Path
from neo4j import GraphDatabase
#from kbs import CONFIG



def read_files(minio_client, bucket,intervals:dict=None, prefix=None, extensions: List[str] = None) -> List[str]:
    """
    Get a list of names of all files in the bucket that match the given prefix and extensions.
    Args:
        client: Minio client object
        bucket: Name of the bucket to search in
        prefix: Prefix to filter the files by
        extensions: List of extensions to filter the files by
    Returns: 
        List of names of all files that match the given criteria
    """
    logging.info("start to get objects in minio")
    logging.info("get objects")
    # objects = MINIO_CLIENT.list_objects(CONFIG["bucket"],recursive=True)
    objects = minio_client.list_objects(bucket, prefix, recursive=True)
    logging.info("get good objects")

    if not objects:
        raise RuntimeError(f"No file found")
    if intervals:
        good_objects = [(obj.object_name, obj.last_modified  ) for obj in objects if obj.object_name.lower().endswith(tuple(extensions)) and datetime.strptime(intervals["start"], "%Y-%m-%d %H:%M:%S")<obj.last_modified.replace(tzinfo=None, microsecond=0) <=  datetime.strptime(intervals["end"], "%Y-%m-%d %H:%M:%S") ]
    else:
        good_objects = [(obj.object_name, obj.last_modified  ) for obj in objects if obj.object_name.lower().endswith(tuple(extensions))]
    sorted_obj = sorted(good_objects, key=lambda x: x[1])
    final_obj = [obj[0] for obj in sorted_obj]
    if not good_objects:
        raise ValueError(f"No files found with good extensions {extensions}")
    return final_obj
  

def push_in_redis(minio_client, objects):
    """ """ 
    logging.info("start to push data in redis")
    for obj in objects:
        #file = MINIO_CLIENT.get_object(CONFIG["bucket"], obj)
        file = minio_client.get_object("extracted", obj)
        data = json.load(io.BytesIO(file.data))
        print(data)
        #REDIS_CLIENT.json().set(obj.split()[-1], path.Path.root_path(), data)
    
        print(f"{obj.split()[-1]} in redis")

def load_from_redis(redis_client, ids):
    """
    """
    data = []
    for id in ids:
        response=redis_client.execute_command('JSON.GET', id)
        data.append(json.loads(response))
    return data


def save_files_minio(minio_client, data, filename, bucket):
    """
    
    """
    logging.info("Starting to save files into Minio")
    found = minio_client.bucket_exists(bucket)
    if not found:
        minio_client.make_bucket(bucket)
        logging.info(f"{bucket} created")
    if isinstance(data, dict) or isinstance(data, list):
        serialize_data = json.dumps(data)
        minio_client.put_object(
        bucket_name=bucket,
        object_name=filename,
        data=io.BytesIO(serialize_data.encode('utf-8')),
        length=len(serialize_data),
        content_type='application/json'
    )


def save_in_minio(minio_client, location: Path):
    name = location.name
    try:
        result = minio_client.fput_object(
            "images",  # Name of the bucket
            f"{name}",  # Object name in the bucket
            location,
        )
        return result.object_name
    except Exception as exc:
        return None