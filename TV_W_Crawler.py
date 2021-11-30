from datetime import timedelta, datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from docker.types import Mount


DATA_DIR = Path("/home/Database/")
OUTPUT_FILE_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S.%f%z"
DOCKER_IMAGE = "wtvpipe"

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "Hauser Fabian",
    "depends_on_past": False,
    "email": ["hauserfabian@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "auto_remove": True,
    "mount_tmp_dir": False,
}

dag = DAG(
    "w_crawler",
    default_args=default_args,
    description="Daily Crawler für TV Daten",
    catchup=False, 
    schedule_interval="0 22 * * 6",
    start_date=datetime(2021, 7, 30),
    tags=["python", "chrome", "TV"],
    concurrency=5,
)
tvcredentials = {"tv_username":"{{ var.value.tv_username }}", 
            "tv_pw_PASSWORD":"{{ var.value.tv_pw_PASSWORD }}",
            "tv_crawlersteuerung":"/home/Database/TV_Database/Crawler-Steuerung.csv"}

with dag:
  default_mounts = [Mount("/home/Database", "/home/Database", type="bind")]

  monthlydrollup = DockerOperator(
        task_id="monthlydrollup",
        image=DOCKER_IMAGE,
        environment=None,
        command=["monthlydrollup"],
        mounts=default_mounts,
    )
  weeklydrollup = DockerOperator(
        task_id="weeklydrollup",
        image=DOCKER_IMAGE,
        environment=None,
        command=["weeklydrollup"],
        mounts=default_mounts,
    )
    
  forecast_png1 = DockerOperator(
        task_id="forecast_png1",
        image="dtvpipe",
        environment=None,
        command=["forecast_png1"],
        mounts=default_mounts,
    )    
    
  crosscorr = DockerOperator(
        task_id="crosscorr",
        image=DOCKER_IMAGE,
        environment=None,
        command=["crosscorr"],
        mounts=default_mounts,
    )
  data_monthly_prep = DockerOperator(
        task_id="data_monthly_prep",
        image=DOCKER_IMAGE,
        environment=None,
        command=["data_monthly_prep"],
        mounts=default_mounts,
    )
  dailydrollup = DockerOperator(
        task_id="dailydrollup",
        image=DOCKER_IMAGE,
        environment=None,
        command=["dailydrollup"],
        mounts=default_mounts,
    )
    

forecast_png1 >> [dailydrollup, weeklydrollup, monthlydrollup] >> crosscorr >> data_monthly_prep
