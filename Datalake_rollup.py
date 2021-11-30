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
DOCKER_IMAGE = "datalakerollup"

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "Hauser Fabian",
    "depends_on_past": False,
    "email": ["hauserfabian@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "auto_remove": True,
    "mount_tmp_dir": False,
}

dag = DAG(
    "datalake_rollup",
    default_args=default_args,
    description="Datalake rollup",
    catchup=False, 
    schedule_interval="00 02 * * *",
    start_date=datetime(2021, 7, 30),
    tags=["python", "Datalake", "Azure", "Facebook"],
    concurrency=3,
)

with dag:
  default_mounts = [Mount("/home/Database", "/home/Database", type="bind")]

  fbcampaigns_rollup_fbvideo = DockerOperator(
        task_id="fbcampaigns_rollup_fbvideo",
        image=DOCKER_IMAGE,
        environment=None,
        command=["fbcampaigns_rollup_fbvideo"],
        mounts=default_mounts,
    )

  fbcampaigns_rollup_fbads = DockerOperator(
        task_id="fbcampaigns_rollup_fbads",
        image=DOCKER_IMAGE,
        environment=None,
        command=["fbcampaigns_rollup_fbads"],
        mounts=default_mounts,
    )    

  fbcampaigns_rollup_fbagegender = DockerOperator(
        task_id="fbcampaigns_rollup_fbagegender",
        image=DOCKER_IMAGE,
        environment=None,
        command=["fbcampaigns_rollup_fbagegender"],
        mounts=default_mounts,
    )
    
  fbcampaigns_rollup_fbcampaign = DockerOperator(
        task_id="fbcampaigns_rollup_fbcampaign",
        image=DOCKER_IMAGE,
        environment=None,
        command=["fbcampaigns_rollup_fbcampaign"],
        mounts=default_mounts,
    )

  fbcampaigns_rollup_fbconversion = DockerOperator(
        task_id="fbcampaigns_rollup_fbconversion",
        image=DOCKER_IMAGE,
        environment=None,
        command=["fbcampaigns_rollup_fbconversion"],
        mounts=default_mounts,
    )

  fbcampaigns_rollup_fbgeo = DockerOperator(
        task_id="fbcampaigns_rollup_fbgeo",
        image=DOCKER_IMAGE,
        environment=None,
        command=["fbcampaigns_rollup_fbgeo"],
        mounts=default_mounts,
    )

  fbcampaigns_rollup_getlist = DockerOperator(
        task_id="fbcampaigns_rollup_getlist",
        image=DOCKER_IMAGE,
        environment=None,
        command=["fbcampaigns_rollup_getlist"],
        mounts=default_mounts,
    )


    
fbcampaigns_rollup_getlist >> [fbcampaigns_rollup_fbvideo, fbcampaigns_rollup_fbads, fbcampaigns_rollup_fbagegender, fbcampaigns_rollup_fbcampaign, fbcampaigns_rollup_fbconversion, fbcampaigns_rollup_fbgeo]
