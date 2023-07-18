# Standard library imports
import datetime as dt 
import json
import logging
import pytz
import os
from .common_utils import create_and_get_folderpath_in_run_folder
# Third party imports
import pandas as pd
from AssetDataReader.AssetDataReader import AssetDataReader
from azure.eventhub import EventHubProducerClient, EventData

# Local application imports
from .common_utils import get_from_context
OUTPUTS_FOLDER=".outputs"
ASSET_PLATFORM_MAPPING= {"ACT": "ACT"
                        ,"Cadonal": "Cadonal"
                        ,"Calgary": "CDHI"
                        ,"Estrellada": "Estrellada"
                        ,"Helioenergy 1": "Helioenergy"
                        ,"Helioenergy 2": "Helioenergy"
                        ,"Helios 1": "Helios"
                        ,"Helios 2": "Helios"
                        ,"Kaxu": "Kaxu"
                        ,"Mojave Alpha": "Mojave"
                        ,"Mojave Beta": "Mojave"
                        ,"Palmatir": "Palmatir"
                        ,"Quadra": "Quadra"
                        ,"San Pedro": "San Pedro"
                        ,"Solaben 1": "Solaben"
                        ,"Solaben 2": "Solaben"
                        ,"Solaben 3": "Solaben"
                        ,"Solaben 6": "Solaben"
                        ,"Solacor 1": "Solacor"
                        ,"Solacor 2": "Solacor"
                        ,"Solana": "Solana"
                        ,"Solnova 1": "Solucar"
                        ,"Solnova 3": "Solucar"
                        ,"Solnova 4": "Solucar"
                        ,"PS 10": "Solucar"
                        ,"PS 20": "Solucar"
                        ,"Sevilla PV": "Solucar"
                        }

pd.options.mode.chained_assignment = None  # default='warn'

def get_tag_list_from_list(tags_list):
    taglist=[]
    for tag in tags_list:
        taglist.append({"descriptor":"","tag":tag})
    return taglist


def get_tag_list_from_excel(tags_file_path):
    
    taglist=[]
    
    config_folder = get_from_context("config_folder")
    tags_file_path = os.path.join(config_folder, tags_file_path)

    logging.info(f"Reading config file '{tags_file_path}'")
    if os.path.exists(tags_file_path):
        df_tags=pd.read_excel(tags_file_path)

        #Server	PIPointName	Unit	Description	Platform
        taglist = [{"descriptor":"","tag":row['PIPointName']} for _,row in df_tags.iterrows()]

    else:
        raise FileNotFoundError(f"no file at '{tags_file_path}'")

    return taglist

def read_pi_data(asset,tagfile,utc_date_from_str,utc_date_to_str,time_interval):
    """ Read data from PI

    return df with all data
    """
    df = pd.DataFrame()

    IN_AIRFLOW = os.getenv('AIRFLOW_HOME')
    if IN_AIRFLOW:
        from airflow.hooks.base_hook import BaseHook
        connection = BaseHook.get_connection("ASI_PI_CONNECTION")
        PI_USERNAME = connection.login
        PI_PASSWORD = connection.password        
    else:
        from dotenv import load_dotenv 
        load_dotenv()  # take environment variables from .env.
        PI_USERNAME = os.getenv("PI_USERNAME")
        PI_PASSWORD = os.getenv("PI_PASSWORD")

    piaf_user = PI_USERNAME
    piaf_pwd = PI_PASSWORD

    #tag_list = get_tag_list_from_excel(tagfile)
    tag_list = get_tag_list_from_list(tagfile)


    platform = ASSET_PLATFORM_MAPPING[asset]
    
    local_time_zone = pytz.timezone(AssetDataReader.get_asset_definition(platform)['time_zone'])

    utc_time_zone = pytz.timezone("UTC")
    if utc_time_zone != local_time_zone:
        utc_date_from = dt.datetime.strptime(utc_date_from_str, '%Y-%m-%dT%H:%M:%S')
        utc_date_to = dt.datetime.strptime(utc_date_to_str, '%Y-%m-%dT%H:%M:%S')
        local_date_from = utc_time_zone.localize(utc_date_from).astimezone(local_time_zone)
        local_date_to = utc_time_zone.localize(utc_date_to).astimezone(local_time_zone)
        local_date_from_str = local_date_from.strftime("%Y-%m-%dT%H:%M:%S")
        local_date_to_str = local_date_to.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        local_date_from_str = utc_date_from_str
        local_date_to_str = utc_date_to_str

    df, _ = AssetDataReader.read_asset_data (
        asset=asset,
        date_from = local_date_from_str,
        date_to = local_date_to_str,
        tag_list = tag_list,
        download_mode = 'interpolated',
        time_interval = time_interval,
        mode = 'wide',
        local_time = False,
        on_error_attempts=4,
        piaf_user = piaf_user,
        piaf_pwd = piaf_pwd,
        verbose=True
    )
    #To avoid Exception:
    #ValueError: Excel does not support datetimes with timezones. Please ensure that datetimes are timezone unaware before writing to Excel.
    # Remove timezone from columns
    df['time'] = df['time'].dt.tz_localize(None)
    
    if not df.empty:
        logging.info(f"Downloaded {len(df.axes[0])} records for {len(df.axes[1]) - 1} tags")
    else:
        logging.warning(f"No data returned")
    
    return df

def send_data_to_pi(df_to_send,tag,asset_connection):
    """ Send data to target PI resource

    param df_to_send: (DF) target data to send with the correct format
    param tag: (str) Whole path PI-Point or PIAF Path to write into
    param asset_connection: (str) PI Web API Endpoint
    """

    IN_AIRFLOW = os.getenv('AIRFLOW_HOME')
    if IN_AIRFLOW:
        from airflow.hooks.base_hook import BaseHook
        connection = BaseHook.get_connection("ASI_PI_EH_DATA_CONNECTION")
        extra_json=json.loads(connection.extra)
        ASI_PI_EH_DATA_CONNECTION_STR = extra_json["connection string"]
        ASI_PI_EH_DATA_NAME= extra_json["name"]        
    else:
        from dotenv import load_dotenv 
        load_dotenv()  # take environment variables from .env.
        ASI_PI_EH_DATA_CONNECTION_STR = os.getenv("ASI_PI_EH_DATA_CONNECTION_STR")
        ASI_PI_EH_DATA_NAME = os.getenv("ASI_PI_EH_DATA_NAME")

    # Create Event Hub Client
    client = EventHubProducerClient.from_connection_string(ASI_PI_EH_DATA_CONNECTION_STR, eventhub_name = ASI_PI_EH_DATA_NAME)

    event_body = "{'meta':{},'body':{'asset_type':'pi','asset_connection':'%s','tag':'%s','content':%s}}" % (asset_connection,tag.replace("\\",'\\\\'),df_to_send.to_json(orient="records",date_format='iso').replace('"',"'"))

    # Prepare batch to write
    event_data_batch = client.create_batch()
    event_data_batch.add(EventData(event_body))

    # Write batch
    with client:
        client.send_batch(event_data_batch)

# def read_data(asset,tags,date_from,date_to,time_interval):
#     """ Read all pump needed input data from PI

#     param config: configuration parameters
#     return df with all input data for whole pump
#     """
    
#     local_time_zone = pytz.timezone(AssetDataReader.get_asset_definition(asset)['time_zone'])
#     utc_time_zone = pytz.timezone("UTC")
    
#     date_from = utc_time_zone.localize(dt.datetime.strptime(date_from,"%Y-%m-%dT%H:%M:%S")).astimezone(local_time_zone)
#     date_to = utc_time_zone.localize(dt.datetime.strptime(date_to,"%Y-%m-%dT%H:%M:%S")).astimezone(local_time_zone)
    
#     date_from = date_from.strftime("%Y-%m-%dT%H:%M:%S")
#     date_to = date_to.strftime("%Y-%m-%dT%H:%M:%S")
    
#     tag_list = []
#     for tag in tags:
#         tag_list += [{"descriptor":"","tag":tag}]

#     df_input , _ = AssetDataReader.read_asset_data (
#         asset=asset,
#         date_from = date_from,
#         date_to = date_to,
#         tag_list = tag_list,
#         download_mode = 'interpolated',
#         time_interval = time_interval,
#         mode = 'wide',
#         local_time = False,
#         on_error_attempts=4,
#         verbose = True
#     )

#     return df_input