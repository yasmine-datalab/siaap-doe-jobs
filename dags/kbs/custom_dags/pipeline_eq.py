from __future__ import annotations
import os
from typing import List
import io
from datetime import datetime
import pandas as pd
import logging
import json
from minio import Minio
from minio.commonconfig import REPLACE, CopySource
import redis
from redis.commands.json import path
from airflow.models.baseoperator import chain
import uuid
from airflow.decorators import task, dag
from airflow.models import Variable
from neomodel import config as neo4j_db_config
from kbs.common.neo4j_models import process_equipement_data, process_schema_file, process_factory_data, Usine
from kbs.common.rw import save_files_minio, read_files
from kbs.common.pdf_processor import extract_pdf_data
from kbs.common.excel_extractor import extract_excel_data
from kbs.common.word_extractor import extract_word_data




REDIS_HOST = Variable.get("REDIS_HOST")
REDIS_PORT = Variable.get("REDIS_PORT")
MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY= Variable.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")


REDIS_CLIENT = redis.Redis(
    host= REDIS_HOST, port= REDIS_PORT, db=0
   )
REDIS_CLIENT_1 = redis.Redis(
    host= REDIS_HOST, port= REDIS_PORT, db=1
   )

MINIO_CLIENT = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

NEO4J_PROTOCOL = Variable.get("NEO4J_PROTOCOL")
NEO4J_USERNAME = Variable.get("NEO4J_USERNAME")
NEO4J_PASSWORD = Variable.get("NEO4J_PASSWORD")
NEO4J_HOSTNAME = Variable.get("NEO4J_HOSTNAME")
NEO4J_PORT = Variable.get("NEO4J_PORT")
NEO4J_DB = Variable.get("NEO4J_DB")

BUCKET_NAME = "siaap-doe"
EXTRACTED_BUCKET_NAME = "extracted"
SCHEMA_FILES_BUCKET_PREFIX = (
        "DOE_SEM/SEM_Eng/Engineering - Technique/111500 global station/DOCUMENTS PROCESS/SCHEMAS PID/"
    )

FACTORY_BUCKET_NAME = "factory"
FACTORY_FILE_PATH = "factories.json"
ACTIVE_FACTORY_DOE_FOLDER = "DOE_SEM"

# Initialize DB connections
neo4j_db_config.DATABASE_URL = f"{NEO4J_PROTOCOL}://{NEO4J_USERNAME}:{NEO4J_PASSWORD}@{NEO4J_HOSTNAME}:{NEO4J_PORT}"  # default
neo4j_db_config.DATABASE_NAME = NEO4J_DB
default_args = {
    'start_date': datetime(2024, 7, 1),
    'depends_on_past': True
}

@dag(dag_id= "pipeline_eq", default_args= default_args, catchup=True, schedule_interval=None)
    
def equipement_process():
    # @task
    # def gen_dates(exec_date):
    #     """ """
    #     dates = {"start": exec_date.split(".")[0], "end": datetime.now().strftime("%Y-%m-%d %H:%M:%S") }
    #     #dates = {"start": "2023-03-01 00:00:00", "end": datetime.now().strftime("%Y-%m-%d %H:%M:%S") }
    #     filename = f"dates_{exec_date.split()[0].replace('-', '')}"
    #     save_files_minio(MINIO_CLIENT, dates, filename+".json", "intervalles")
    #     return filename


    @task
    def read_(bucket, extensions: List[str] = None) -> List[str]:
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
        # objects = MINIO_CLIENT.list_objects(CONFIG["bucket"],recursive=True)
        objects = read_files(minio_client=MINIO_CLIENT,bucket= bucket, intervals=None, extensions=extensions )
        BATCH_SIZE = 1000
        if not objects:
            logging.info("No files found.")
            return
        batches = [objects[i:i + BATCH_SIZE] for i in range(0, len(objects), BATCH_SIZE)]
        print(len(batches))
        batch_files = []
        for index, batch in enumerate(batches):
            batch_file = f"{extensions[0].replace(".","")}_{index}.json"
            print(batch_file)
            save_files_minio(MINIO_CLIENT, batch,filename=batch_file, bucket=bucket)
            batch_files.append(batch_file)
        print(batch_files)
        return batch_files
    
    @task
    def push_data(file: str, bucket: str):
        """ """ 
        logging.info("start to push data in redis")
        logging.info("get file from minio")

        file_obj = MINIO_CLIENT.get_object(bucket, file)
        obj_names = json.load(io.BytesIO(file_obj.data))
  
        for obj in obj_names:
            name = obj.split('/')[-1]
            file_path = f'tmp/{name}'
            file_treat = MINIO_CLIENT.fget_object(bucket_name='siaap-doe', object_name=obj, file_path=file_path)
            print(file_treat)
            key = str(uuid.uuid4())
        
                
            if obj.endswith((".pdf")):
                #file_in_bio = io.BytesIO(data_file.data)
    
                #read_files(MINIO_CLIENT, "siaap-doe", prefix="pdf-offset", extensions=[".txt"])
                index = file.split(".")[0].split("_")[-1]
                
                offset = MINIO_CLIENT.get_object(bucket, f"pdf-offset-{index}.txt")
                try:
                    offset_lst = json.load(io.BytesIO(offset.data)) 
                except Exception as e:
                    offset_lst = []
                
            
                logging.info(f"nbre de fichiers déjà traités: {len(offset_lst)}")
                if obj not in offset_lst:
                    try:
                        data = extract_pdf_data(MINIO_CLIENT, file_path, obj)
                    except Exception as e:
                        err = {"file": obj, "exception": e}
                        save_files_minio(MINIO_CLIENT, err, "pdf_err/{obj}")
                        continue
                    
                    if data != {}:
                        logging.info("in minio")
                        # if obj.startswith(SCHEMA_FILES_BUCKET_PREFIX):
                        #     logging.info(obj)
                        #     schema_filename = obj.split("/")[-1]
                            
                        #     schema_pid = get_pid_from_filename(schema_filename)
                        #     logging.info("process schema file")
                        #     process_schema_file(
                        #             schema_pid=schema_pid, filename=schema_filename, full_path=obj, usine_node=main_usine
                        #         )
                        #     print(f"SCHEMA -- SUCCESS -- Processed: {schema_filename}")
                    
                        REDIS_CLIENT_1.json().set(key, path.Path.root_path(), data)
                        logging.info("end to push into redis")
                        offset_lst.append(obj)
                        save_files_minio(MINIO_CLIENT, offset_lst, f"pdf-offset-{index}.txt", "siaap-doe")
                
            elif obj.endswith((".xls", ".xlsx")):
                index = file.split(".")[0].split("_")[-1]
                
                offset = MINIO_CLIENT.get_object(bucket, f"excel-offset-{index}.txt")
                try:
                    offset_lst = json.load(io.BytesIO(offset.data)) 
                except Exception as e:
                    offset_lst = []
                logging.info(f"nbre de fichiers déjà traités: {len(offset_lst)}")
                if obj not in offset_lst:
                    try:    
                        data = extract_excel_data(MINIO_CLIENT, file_path, obj)
                    except Exception as e:
                        err = {"file": obj, "exception": e}
                        save_files_minio(MINIO_CLIENT, err, "excel_err/{obj}")
                        continue
                
                REDIS_CLIENT_1.json().set(key, path.Path.root_path(), data)
                logging.info("end to push into redis")
                offset_lst.append(obj)
                save_files_minio(MINIO_CLIENT, offset_lst, f"excel-offset-{index}.txt", "siaap-doe")
                
            elif obj.endswith((".doc", ".docx")):
                index = file.split(".")[0].split("_")[-1]
                
                offset = MINIO_CLIENT.get_object(bucket, f"doc-offset-{index}.txt")
                try:
                    offset_lst = json.load(io.BytesIO(offset.data)) 
                except Exception as e:
                    offset_lst = []
                logging.info(f"nbre de fichiers déjà traités: {len(offset_lst)}")
                if obj not in offset_lst:
                    try:
                        data = extract_word_data(MINIO_CLIENT, file_path, obj)
                    except:
                        err = {"file": obj, "exception": e}
                        save_files_minio(MINIO_CLIENT, err, "doc_err/{obj}")
                        continue
                REDIS_CLIENT_1.json().set(key, path.Path.root_path(), data)
                logging.info("end to push into redis")
                offset_lst.append(obj)
                save_files_minio(MINIO_CLIENT, offset_lst, f"doc-offset-{index}.txt", "siaap-doe")
                
            else:
                # raise ValueError("file extension not autorized")
                continue
            os.remove(file_path)
            logging.info(f"{file_path} removed")
            logging.info(f"{obj.split()[-1]} in redis")

  
    # @task
    # def process_factory():
    #     # Get factories from minio
    #     factories = MINIO_CLIENT.get_object(FACTORY_BUCKET_NAME, FACTORY_FILE_PATH)
    #     factories_json = json.load(io.BytesIO(factories.data))
    #     for factory in factories_json:
    #             try:
    #                 process_factory_data(data=factory)
    #                 print(f"FACTORIES -- SUCCESS -- Processed: {factory.get('name')}")
    #             except Exception as e:
    #                 print(f"FACTORIES -- FAILURE -- Processing: {factory.get('name')}")
    #                 print(e)
    

    # equipements = read_("extracted",[".json"])
    # push_data.partial(bucket="extracted").expand(file=equipements)
    
    pdfs = read_("siaap-doe",[".pdf"])
    push_data.partial(bucket="siaap-doe").expand(file=pdfs)
    excels = read_("siaap-doe",[".xls", ".xlsx"])
    push_data.partial(bucket="siaap-doe").expand(file=excels)
    words = read_("siaap-doe",[".doc", ".docx"])
    push_data.partial(bucket="siaap-doe").expand(file=words)
    
PIPELINE = equipement_process()