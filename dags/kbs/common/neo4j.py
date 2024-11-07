import json
import io
from pathlib import Path
from neomodel import config as neo4j_db_config
from kbs.common.neo4j_models import process_equipement_data, process_schema_file, process_factory_data, Usine, get_pid_from_filename
from kbs.common.rw import  read_files

BUCKET_NAME = "siaap-doe"
EXTRACTED_BUCKET_NAME = "extracted"
SCHEMA_FILES_BUCKET_PREFIX = (
        "DOE_SEM/SEM_Eng/Engineering - Technique/111500 global station/DOCUMENTS PROCESS/SCHEMAS PID/"
    )

FACTORY_BUCKET_NAME = "factory"
FACTORY_FILE_PATH = "factories.json"
ACTIVE_FACTORY_DOE_FOLDER = "DOE_SEM"

def process_factory(minio_client):
    # Get factories from minio
    factories = minio_client.get_object(FACTORY_BUCKET_NAME, FACTORY_FILE_PATH)
    factories_json = json.load(io.BytesIO(factories.data))
    for factory in factories_json:
            try:
                process_factory_data(data=factory)
                print(f"FACTORIES -- SUCCESS -- Processed: {factory.get('name')}")
            except Exception as e:
                print(f"FACTORIES -- FAILURE -- Processing: {factory.get('name')}")
                print(e)
    
    # Get the only valid Usine based on doe_folder
    main_usine = Usine.nodes.first(doe_folder=ACTIVE_FACTORY_DOE_FOLDER)
    schemas_list = read_files(minio_client, BUCKET_NAME, prefix=SCHEMA_FILES_BUCKET_PREFIX, extensions=[".pdf"])
    # Process Schema files
    for schema_file in schemas_list:
        schema_filename = schema_file.split("/")[-1]
        try:
            schema_pid = get_pid_from_filename(schema_filename)
            process_schema_file(
                schema_pid=schema_pid, filename=schema_filename, full_path=schema_file, usine_node=main_usine
            )
            print(f"SCHEMA -- SUCCESS -- Processed: {schema_filename}")
        except Exception as e:
            print(f"SCHEMA -- FAILURE -- Processing: {schema_filename}")
            print(e)
            raise
    # Get file to be processed lists
    extracted_objects = read_files(minio_client, EXTRACTED_BUCKET_NAME, prefix="extracted/", extensions=[".json"])

    for infile in extracted_objects:
        # Download the file to be processed
        infile_local_path = minio_client.get_object(FACTORY_BUCKET_NAME, FACTORY_FILE_PATH)
        # Get data from file
        file_data = json.load(io.BytesIO(factories.data))
        try:
            file_data = json.load(file_data)
            process_equipement_data(data=file_data, usine_node=main_usine)
            print(f"EQUIPEMENTS -- SUCCESS -- Processed: {infile_local_path.name}")
        except Exception as e:
            print(f"EQUIPEMENTS -- FAILURE -- Processing: {infile_local_path.name}")
            print(e)
            raise