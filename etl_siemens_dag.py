import os
import logging
import errno
import json
from datetime import datetime,timedelta
import zipfile
import sys

import time

from util_etl_siemens import date_from_to_list, download, cloud_sync_fn

from multiprocessing import Pool

NUM_PROCESSES = 12

def worker(input):
    download_summary={}
    upload_summary={}
    download_summary=download(input[0],input[1],input[2])
    upload_summary=cloud_sync_fn(input[0],download_summary,input[1],input[2])

def run_process(request_json=""):
    download_summary={}
    

    # run_folder is this folder
    run_folder=os.path.dirname(os.path.realpath(__file__))

    logging.info(f"received request_json: {request_json}")
    logging.info(f"received run_folder: {run_folder}")
    
    download_jobs=request_json['download_jobs']
    for job in download_jobs:
        filename = job["filename"]
        asset = job["asset"]
        tags = job["tag_list"]
        date_from = job["date_from"]
        date_to = job["date_to"]
        date_from = datetime.strptime(date_from, "%Y-%m-%dT%H:%M:%S")
        date_to = datetime.strptime(date_to, "%Y-%m-%dT%H:%M:%S")
        dates_from,dates_to = date_from_to_list(date_from, date_to, 60)
        
        pool = Pool(processes=NUM_PROCESSES)
        # for i in range(len(dates_from)):
        #     download_summary={}
        #     upload_summary={}
        #     download_summary=download(request_json,dates_from[i],dates_to[i])
        #     upload_summary=cloud_sync_fn(request_json,download_summary,dates_from[i],dates_to[i])

        pool.map(worker, [(request_json,dates_from[i],dates_to[i]) for i in range(len(dates_from))])
                 
        pool.close()
        pool.join()




if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
    request_json = sys.argv[1]
    # is request_json a string or a dict?
    if isinstance(request_json, str):
        request_json=json.loads(request_json)
    else:
        request_json=request_json

    # check how much does the next line take

    start_time = time.time()   
    run_process(request_json)
    elapsed_time_minutes = (time.time() - start_time)/60
    logging.info(f"Elapsed time: {elapsed_time_minutes} minutes")