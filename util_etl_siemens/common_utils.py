# Standard library imports
import errno
import json
import logging
import os

# Third party imports
import zipfile

# Local application imports

CONFIGURATION_FOLDER = "config"

pipeline_context = dict()

IN_AIRFLOW = os.getenv('AIRFLOW_HOME')

def make_safe_filename(s):
    def safe_char(c):
        if c.isalnum():
            return c
        else:
            return "_"
    return "".join(safe_char(c) for c in s).rstrip("_")

def create_and_get_folderpath_in_run_folder(foldername):
    try:
        run_folder = os.path.dirname(os.path.realpath(__file__))
        target_folder = os.path.join(run_folder, foldername )
        if not os.path.isdir(target_folder):
            mkdir_p(target_folder, 0o700)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(target_folder):
            pass
        else:
            raise 
    return target_folder

def mkdir_p(path, mode=0o777,exist_ok=False):
    """
    Create subdirectory hierarchy given in the paths argument.
    """
    try:
        os.makedirs(path, mode,exist_ok=exist_ok)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise 

def initialize_folders(folders):
    for folder in folders:
        mkdir_p(folder, 0o700) 

def get_context():
    
    context = {}
    if IN_AIRFLOW:
        from airflow.operators.python import get_current_context
        try:
            context = get_current_context()
        except Exception as ex:
            logging.error(f"Airflow context not available. Error:{ex}")
            pass 
    else:
        context = pipeline_context
    return context

def get_from_context(item):
    if IN_AIRFLOW:
        from airflow.operators.python import get_current_context
        try:
            context = get_current_context()
            ti = context.get('ti') 
            value = ti.xcom_pull(key=item, task_ids='init_flow_env')
        except Exception as ex:
            logging.error(f"Airflow context not available. Error:{ex}")
            value = None
            pass
    else:
        value=pipeline_context[item]
    return value

def init_workflow_env():
    try:
        if IN_AIRFLOW:
            from airflow.operators.python import get_current_context
            context = get_current_context()
            
            logging.info(f"init_workflow_env. Checking required variables..")
            
            from airflow.contrib.hooks.fs_hook import FSHook
                
            dag_run_id=context['dag_run'].run_id
            dag_id=context['dag'].dag_id
            logging.info(f"dag_id:{dag_id}")
            logging.info(f"dag_run.run_id:{dag_run_id}")
            
            request_json= json.dumps(context['dag_run'].conf) 

            hook = FSHook("fs_processes")
            fshook_basepath = hook.get_path()
            
            dag_process_basepath=os.path.join(fshook_basepath, dag_id)
            dag_conf_folder=os.path.join(dag_process_basepath,CONFIGURATION_FOLDER)

            safe_dag_run_id=make_safe_filename(dag_run_id)
            dag_run_basepath = os.path.join(fshook_basepath, safe_dag_run_id)

            logging.info(f"dag_run_basepath: {dag_run_basepath}")
            logging.info(f"dag_process_basepath: {dag_process_basepath}")
            logging.info(f"dag_conf_folder: {dag_conf_folder}")

            #Checking need for update of config files
            print (f"Checking required config files..")
            current_file_path=os.path.dirname(os.path.realpath(__file__))
            
            logging.info(f"current_file_path: {current_file_path}")
            zip_extension_position = current_file_path.find(".zip")
            logging.info(f"zip_extension_position: {zip_extension_position}")
            
            if (zip_extension_position != -1):
                path_to_zip_file=current_file_path [0:(zip_extension_position + len(".zip"))]
                logging.info(f"path_to_zip_file: {path_to_zip_file}")
               
                logging.info(f"Opening: '{path_to_zip_file}'..")
                with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
                    for name in zip_ref.namelist():
                        print(f"file: '{name}'")
                        if name.startswith(CONFIGURATION_FOLDER):
                            print(f"Checking if exists folder: '{dag_process_basepath}'")
                            if not os.path.isdir(dag_process_basepath):
                                mkdir_p(dag_process_basepath, 0o700)
                                logging.info(f"Created folder {dag_process_basepath}")
                            file_path=os.path.join(dag_process_basepath,name)
                            target_folder = os.path.dirname(file_path)
                            if not os.path.isdir(target_folder):
                                mkdir_p(target_folder, 0o700)
                                logging.info(f"Created folder {target_folder}")                            
                            if not os.path.isfile(file_path):
                                print(f"extracting {name} to {dag_process_basepath}")
                                zip_ref.extract(name,dag_process_basepath) 

            # #save variables in XCOM
            task_instance = context['task_instance']
            task_instance.xcom_push(key="config_folder", value=dag_conf_folder)
            task_instance.xcom_push(key="request_json", value=request_json)
            task_instance.xcom_push(key="run_folder", value=dag_run_basepath)

            # task_instance.xcom_push(key="request", value=request_json)
            return request_json
            
    except Exception as e:
        raise Exception(f"init_workflow_env. Error{e}")   
