import json
import os
import datetime as dt 
import shutil
import logging
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta

from util_etl_siemens.pi_data import read_pi_data
from util_etl_siemens.cloud_sync import azureblob_upload
from util_etl_siemens.common_utils import get_from_context, create_and_get_folderpath_in_run_folder

CACHE_FOLDER = ".cache"
DEBUG_LOGS_FOLDER = ".debuglogs"
OUTPUTS_FOLDER = ".outputs"

AA_REPORT_FOLDER="siemens"
AA_OUTPUTS_FOLDER ="{asset}/{year}/{month}/{day}/{filename}"
AA_OUTPUT_FILENAME_TEMPLATE ="{year}-{month}-{day}T{hour}{minutes}{seconds}_{filename}"

def date_from_to_list(date_from,date_to,n_intervals=60,time_interval=1):
    # n_intervals in num of intervals
    # time_interval in minutes
    new_date = date_from
    dates_interval_from = []
    while new_date < date_to:
        dates_interval_from.append(new_date)
        new_date = new_date + timedelta(minutes = n_intervals)
    for i in range(len(dates_interval_from)):
        dates_interval_from[i] = dates_interval_from[i].strftime("%Y-%m-%dT%H:%M:%S")
    dates_interval_to = []
    new_date = date_from - timedelta(minutes = time_interval) # to avoid overlapping with next interval
    while new_date < date_to:
        new_date = new_date + timedelta(minutes = n_intervals)
        dates_interval_to.append(new_date)
    for i in range(len(dates_interval_to)):
        dates_interval_to[i] = dates_interval_to[i].strftime("%Y-%m-%dT%H:%M:%S")
    return dates_interval_from,dates_interval_to

def data_download(asset,tags,date_from_str,date_to_str,time_interval,filename,request_json):
    file_path="" 
    if isinstance(request_json, str):
        request_json=json.loads(request_json)
    else:
        request_json=request_json

    #download data
    try:
        df = read_pi_data(asset,tags,date_from_str,date_to_str,time_interval)
    except Exception as e:
        logging.error("download_task: %s", e)
    
    
    df = df.rename(columns={'time': 'TIMESTAMP'})

    date_local=dt.datetime.strptime(date_from_str, "%Y-%m-%dT%H:%M:%S")
    year=date_local.year
    month=date_local.strftime("%m")
    day=date_local.strftime("%d")
    year=date_local.year
    month=date_local.strftime("%m")
    day=date_local.strftime("%d")
    hour = date_local.strftime("%H")
    minutes = date_local.strftime("%M")
    seconds = date_local.strftime("%S")
    output_filename=AA_OUTPUT_FILENAME_TEMPLATE.format(year=year,month=month,day=day,hour=hour,minutes=minutes,seconds=seconds,filename=f"{filename}.csv")
    folder_path = create_and_get_folderpath_in_run_folder(os.path.join(OUTPUTS_FOLDER,str(year),month,day))
    file_path=os.path.join(folder_path,output_filename)
    # if not os.path.exists(os.path.join(run_folder,OUTPUTS_FOLDER)):
    #     os.makedirs(os.path.join(run_folder,OUTPUTS_FOLDER))
    df.to_csv(file_path,index=False)
    return file_path    

def download(request_json, date_from, date_to):
    download_summary={}
    if isinstance(request_json, str):
        request_json=json.loads(request_json)
    else:
        request_json=request_json

    run_folder=os.path.dirname(os.path.realpath(__file__)) 

    # logging.info(f"received request_json: {request_json}")
    # logging.info(f"received run_folder: {run_folder}")

    download_jobs=request_json['download_jobs']
    for job in download_jobs:
        filename = job["filename"]
        asset = job["asset"]
        tag_list = job["tag_list"]
        time_interval = job["time_interval"]
        download_mode = job["download_mode"]
        file_extension = job["file_extension"]
        logging.info(f"Downloading {filename} between {date_from} and {date_to}")
        file_path = data_download(asset,tag_list,date_from,date_to,time_interval,filename,request_json)
        if file_path!="":
            download_summary[filename] = file_path

    return download_summary   


def cloud_sync_fn(request,download_summary,date_from,date_to):
    
    logging.info(f"Uploading {len(download_summary)} files to cloud")

    if isinstance(request, str):
        request_json=json.loads(request)
    else:
        request_json=request

        
    download_jobs=request_json['download_jobs']
    for job in download_jobs:
        filename = job["filename"]
        asset = job["asset"]
        local_file_path=download_summary[filename]

        date_local=dt.datetime.fromisoformat(date_from)
        year=date_local.year
        month=date_local.strftime("%m")
        day=date_local.strftime("%d")
        hour = date_local.strftime("%H")
        minutes = date_local.strftime("%M")
        seconds = date_local.strftime("%S")
        output_filename=AA_OUTPUT_FILENAME_TEMPLATE.format(year=year,month=month,day=day,hour=hour,minutes=minutes,seconds=seconds,filename=f"{filename}.csv")
        cloud_file_path = AA_OUTPUTS_FOLDER.format(asset=asset, year=year, month=month, day=day, filename=output_filename)
        blob_file_path=cloud_file_path
        try:
            azureblob_upload(local_file_path,blob_file_path)
            logging.info(f"Uploaded file: {blob_file_path}")
        except Exception as e:
            logging.error("Error while uploading file: %s", e)

def multiprocess_task(inputs):
    download_summary={}
    upload_summary={}
    download_summary=download(inputs[0],inputs[1],inputs[2])
    # upload_summary=cloud_sync_fn(request_json,download_summary, date_from, date_to)

            
    

            