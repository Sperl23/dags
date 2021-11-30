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
DOCKER_IMAGE = "mtvpipe"

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "Hauser Fabian",
    "depends_on_past": False,
    "email": ["hauserfabian@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "auto_remove": True,
    "mount_tmp_dir": False,
}

dag = DAG(
    "m_crawler",
    default_args=default_args,
    description="Daily Crawler für TV Daten",
    catchup=False, 
    schedule_interval="0 18 20 * *",
    start_date=datetime(2021, 9, 30),
    tags=["python", "chrome", "TV"],
    concurrency=3,
)

tvcredentials = {"tv_username":"{{ var.value.tv_username }}", 
            "tv_pw_PASSWORD":"{{ var.value.tv_pw_PASSWORD }}",
            "tv_crawlersteuerung":"/home/Database/TV_Database/Crawler-Steuerung.csv"}

with dag:
  default_mounts = [Mount("/home/Database", "/home/Database", type="bind")]

  eco_crawler = DockerOperator(
        task_id="eco_crawler",
        image=DOCKER_IMAGE,
        environment=tvcredentials,
        command=["eco_crawler"],
        mounts=default_mounts,
    )

eco_crawler 
