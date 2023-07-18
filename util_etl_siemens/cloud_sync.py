import datetime
import json
import logging
import os

from azure.storage.blob import BlobServiceClient
import azure.storage.blob.aio as storage
import asyncio

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
if AIRFLOW_HOME:
    from airflow.hooks.base_hook import BaseHook
    connection = BaseHook.get_connection("ASI_BLOB_CONNECT_STR_SERVICE1")
    extra_json=json.loads(connection.extra)
    BLOB_CONNECT_STR = extra_json["connection string"]
    AZURE_SA_CONTAINER= extra_json["container"]
else:
    from dotenv import load_dotenv 
    load_dotenv()  # take environment variables from .env.
    BLOB_CONNECT_STR = os.getenv("BLOB_CONNECT_STR")
    AZURE_SA_CONTAINER = os.getenv("AZURE_SA_CONTAINER")

class AsyncAzureBlobClient(object):

    # connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    connection_string = BLOB_CONNECT_STR

    async def upload_to_azureblob(self, source_file, target_file):
        # Instantiate a new BlobServiceClient using a connection string
        from azure.storage.blob.aio import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        logging.getLogger("azure.storage.blob").setLevel(logging.WARNING)

        blob_url=""

        async with blob_service_client:
            try:
                # Instantiate a new ContainerClient
                container_client = blob_service_client.get_container_client(AZURE_SA_CONTAINER)
                
                # Instantiate a new BlobClient
                blob_client = container_client.get_blob_client(target_file)

                # Check if exists
                update_required=True
                try:
                    properties = await blob_client.get_blob_properties()
                    blob_length = properties.size
                    file_length = os.path.getsize(source_file)
                    if blob_length==file_length:
                        update_required=False
                        # print(f'Blob length: {blob_length}, File length: {file_length}')
                except Exception as e:
                    logging.error('Error %s', e)
                    
                if update_required:
                    # [START upload_a_blob]
                    # Upload content to block blob
                    with open(source_file, "rb") as data:
                        await blob_client.upload_blob(data, blob_type="BlockBlob", overwrite=True)
                    # [END upload_a_blob]
                else:
                    logging.error(f'Target file {target_file} with same size detected. skipping upload')

                try:
                    blob_url = blob_client.url
                except Exception as e:
                    logging.error('Error %s', e)

            finally:
                pass

        return blob_url
    async def azureblob_exists(self, target_file):
        # Instantiate a new BlobServiceClient using a connection string
        from azure.storage.blob.aio import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        logging.getLogger("azure.storage.blob").setLevel(logging.WARNING)

        exists=False
    
        async with blob_service_client:
            try:
                container_client = blob_service_client.get_container_client(AZURE_SA_CONTAINER)
                
                # Instantiate a new BlobClient
                blob_client = container_client.get_blob_client(target_file)

                try:
                    properties = await blob_client.get_blob_properties()
                    blob_length = properties.size
                    if blob_length>0:
                        print(f'Blob {target_file} length: {blob_length}')
                        exists=True
                except Exception as e:
                    logging.error('Error %s', e)
            except Exception as e:
                logging.error('Error %s', e)
        return exists

    async def azureblob_download(self, source_blob, target_file):
            # Instantiate a new BlobServiceClient using a connection string
            from azure.storage.blob.aio import BlobServiceClient
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            logging.getLogger("azure.storage.blob").setLevel(logging.WARNING)

            async with blob_service_client:
                try:
                    container_client = blob_service_client.get_container_client(AZURE_SA_CONTAINER)
                    # Instantiate a new BlobClient
                    blob_client = container_client.get_blob_client(source_blob)
                    with open(target_file, "wb") as my_blob:
                        stream = await blob_client.download_blob()
                        data = await stream.content_as_bytes()
                        my_blob.write(data)

                except Exception as e:
                    logging.error('Error %s', e)

async def async_upload_to_blob(local_file_name, blob_file_path):
    blobClient = AsyncAzureBlobClient()
    blob_url = await blobClient.upload_to_azureblob(local_file_name, blob_file_path)
    return blob_url

async def async_azureblob_exists(blob_file_path):
    blobClient = AsyncAzureBlobClient()
    exists = await blobClient.azureblob_exists(blob_file_path)
    return exists

async def async_azureblob_download(blob_file_path, target_path):
    blobClient = AsyncAzureBlobClient()
    exists = await blobClient.azureblob_download(blob_file_path, target_path)

def azureblob_exists(target_path):
    exists=False
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        exists = loop.run_until_complete(async_azureblob_exists(target_path))
        logging.info(f'file {target_path} exists= {exists}')
       
    except Exception as ex:
        logging.error('Exception: %s', ex)

    return exists

def azureblob_download(source_path, target_path):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        blob_url = loop.run_until_complete(async_azureblob_download(source_path, target_path))
        logging.info(f'file {source_path} downloaded to {target_path}')
       
    except Exception as ex:
        logging.error('Exception: %s', ex)

def azureblob_upload(source_path, target_path):
    blob_url=""
    try:
        date=datetime.datetime.now()

        filename=source_path

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        blob_url = loop.run_until_complete(async_upload_to_blob(source_path, target_path))
        logging.info(f'file {filename} synced to Azure Blob {blob_url}')


    except Exception as ex:
        logging.error('Exception: %s', ex)

    return blob_url

def azureblob_upload_multiple(target_files,loop = None):
    blob_url=""
    try:
        date=datetime.datetime.now()

        if loop is None:
            print("\n\n------------- LOOP CREATED ---------------\n\n")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        blob_urls = []
        for source_path,target_path in target_files.items():
            filename=source_path
            blob_urls += [asyncio.get_event_loop().run_until_complete(async_upload_to_blob(source_path, target_path))]
            logging.info(f'file {filename} synced to Azure Blob {blob_url}')
       
    except Exception as ex:
        logging.error('Exception: %s', ex)

    return blob_urls

#upload version vieja
def upload(source_path, target_path):
    blob_url=""
    try:
        date=datetime.datetime.now()

        filename=source_path

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        blob_url = loop.run_until_complete(async_upload_to_blob(source_path, target_path))
        logging.info(f'file {filename} synced to Azure Blob {blob_url}')
       
    except Exception as ex:
        logging.error('Exception: %s', ex)

    return blob_url
