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
PYTHON_IMAGE = "dtvpipe"
R_IMAGE = "bubble_indicator"

path = "/home/user2/Bubble1-12/"

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
    "d_crawler",
    default_args=default_args,
    description="Daily Crawler für TV Daten",
    catchup=False, 
    schedule_interval="0 3 * * 2-7",
    start_date=datetime(2021, 7, 30),
    tags=["python", "chrome", "TV", "R", "Bubble Indicator"],
    concurrency=15,
)

tvcredentials = {"tv_username":"{{ var.value.tv_username }}", 
            "tv_pw_PASSWORD":"{{ var.value.tv_pw_PASSWORD }}",
            "tv_crawlersteuerung":"/home/Database/TV_Database/Crawler-Steuerung.csv"}

with dag:
  default_mounts = [Mount("/home/Database", "/home/Database", type="bind")]
  
  
  getlist = DockerOperator(
        task_id="getlist",
        image=PYTHON_IMAGE,
        environment=tvcredentials,
        command=["getlist"],
        mounts=default_mounts,
    )
  dcrawler1 = DockerOperator(
        task_id="dcrawler1",
        image=PYTHON_IMAGE,
        environment=tvcredentials,
        command=["dcrawler1"],
        mounts=default_mounts,
    )
  dcrawler2 = DockerOperator(
        task_id="dcrawler2",
        image=PYTHON_IMAGE,
        environment=tvcredentials,
        command=["dcrawler2"],
        mounts=default_mounts,
    )
  dcrawler3 = DockerOperator(
        task_id="dcrawler3",
        image=PYTHON_IMAGE,
        environment=tvcredentials,
        command=["dcrawler3"],
        mounts=default_mounts,
    )
  dcrawler4 = DockerOperator(
        task_id="dcrawler4",
        image=PYTHON_IMAGE,
        environment=tvcredentials,
        command=["dcrawler4"],
        mounts=default_mounts,
    )
  dcrawler5 = DockerOperator(
        task_id="dcrawler5",
        image=PYTHON_IMAGE,
        environment=tvcredentials,
        command=["dcrawler5"],
        mounts=default_mounts,
    )
  hcrawler = DockerOperator(
        task_id="hcrawler",
        image=PYTHON_IMAGE,
        environment=tvcredentials,
        command=["hcrawler"],
        mounts=default_mounts,
    )
  mcrawler = DockerOperator(
        task_id="mcrawler",
        image=PYTHON_IMAGE,
        environment=tvcredentials,
        command=["mcrawler"],
        mounts=default_mounts,
    )
  dsplit = DockerOperator(
        task_id="dsplit",
        image=PYTHON_IMAGE,
        environment=None,
        command=["dsplit"],
        mounts=default_mounts,
    )
  drollup1 = DockerOperator(
        task_id="drollup1",
        image=PYTHON_IMAGE,
        environment=None,
        command=["drollup1"],
        mounts=default_mounts,
    )
  drollup2 = DockerOperator(
        task_id="drollup2",
        image=PYTHON_IMAGE,
        environment=None,
        command=["drollup2"],
        mounts=default_mounts,
    )
  drollup3 = DockerOperator(
        task_id="drollup3",
        image=PYTHON_IMAGE,
        environment=None,
        command=["drollup3"],
        mounts=default_mounts,
    )
  drollup4 = DockerOperator(
        task_id="drollup4",
        image=PYTHON_IMAGE,
        environment=None,
        command=["drollup4"],
        mounts=default_mounts,
    )
  drollup5 = DockerOperator(
        task_id="drollup5",
        image=PYTHON_IMAGE,
        environment=None,
        command=["drollup5"],
        mounts=default_mounts,
    )
  hrollup = DockerOperator(
        task_id="hrollup",
        image=PYTHON_IMAGE,
        environment=None,
        command=["hrollup"],
        mounts=default_mounts,
    )
  mrollup = DockerOperator(
        task_id="mrollup",
        image=PYTHON_IMAGE,
        environment=None,
        command=["mrollup"],
        mounts=default_mounts,
    )
  # monthlydrollup = DockerOperator(
  #       task_id="monthlydrollup",
  #       image=PYTHON_IMAGE,
  #       environment=None,
  #       command=["monthlydrollup"],
  #       mounts=default_mounts,
  #   )
  # weeklydrollup = DockerOperator(
  #       task_id="weeklydrollup",
  #       image=PYTHON_IMAGE,
  #       environment=None,
  #       command=["weeklydrollup"],
  #       mounts=default_mounts,
  #   )
  rollup_metrics = DockerOperator(
        task_id="rollup_metrics",
        image=PYTHON_IMAGE,
        environment=None,
        command=["rollup_metrics"],
        mounts=default_mounts,
    )
  insider = DockerOperator(
        task_id="insider",
        image=PYTHON_IMAGE,
        environment=None,
        command=["insider"],
        mounts=default_mounts,
    )
  insider_rollup = DockerOperator(
        task_id="insider_rollup",
        image=PYTHON_IMAGE,
        environment=None,
        command=["insider_rollup"],
        mounts=default_mounts,
    )
  chart_plotter = DockerOperator(
        task_id="chart_plotter",
        image=PYTHON_IMAGE,
        environment=None,
        command=["chart_plotter"],
        mounts=default_mounts,
    )
  dailydrollupvalue = DockerOperator(
        task_id="dailydrollupvalue",
        image=PYTHON_IMAGE,
        environment=None,
        command=["dailydrollupvalue"],
        mounts=default_mounts,
    )
  bubblerollup = DockerOperator(
        task_id="bubblerollup",
        image=PYTHON_IMAGE,
        environment=None,
        command=["bubblerollup"],
        mounts=default_mounts,
    )
# R Image
  READ_1 = DockerOperator(
        task_id="READ_1",
        image=R_IMAGE,
        environment=None,
        command=[path+"READ_1.R"],
        mounts=default_mounts,
    )
  READ_2 = DockerOperator(
        task_id="READ_2",
        image=R_IMAGE,
        environment=None,
        command=[path+"READ_2.R"],
        mounts=default_mounts,
    )
  READ_3 = DockerOperator(
        task_id="READ_3",
        image=R_IMAGE,
        environment=None,
        command=[path+"READ_3.R"],
        mounts=default_mounts,
    )
  READ_4 = DockerOperator(
        task_id="READ_4",
        image=R_IMAGE,
        environment=None,
        command=[path+"READ_4.R"],
        mounts=default_mounts,
    )
  READ_5 = DockerOperator(
        task_id="READ_5",
        image=R_IMAGE,
        environment=None,
        command=[path+"READ_5.R"],
        mounts=default_mounts,
    )
  READ_6 = DockerOperator(
        task_id="READ_6",
        image=R_IMAGE,
        environment=None,
        command=[path+"READ_6.R"],
        mounts=default_mounts,
    )
  READ_7 = DockerOperator(
        task_id="READ_7",
        image=R_IMAGE,
        environment=None,
        command=[path+"READ_7.R"],
        mounts=default_mounts,
    )
  READ_8 = DockerOperator(
        task_id="READ_8",
        image=R_IMAGE,
        environment=None,
        command=[path+"READ_8.R"],
        mounts=default_mounts,
    )
  READ_9 = DockerOperator(
        task_id="READ_9",
        image=R_IMAGE,
        environment=None,
        command=[path+"READ_9.R"],
        mounts=default_mounts,
    )
  READ_10 = DockerOperator(
        task_id="READ_10",
        image=R_IMAGE,
        environment=None,
        command=[path+"READ_10.R"],
        mounts=default_mounts,
    )
  READ_11 = DockerOperator(
        task_id="READ_11",
        image=R_IMAGE,
        environment=None,
        command=[path+"READ_11.R"],
        mounts=default_mounts,
    )
  READ_12 = DockerOperator(
        task_id="READ_12",
        image=R_IMAGE,
        environment=None,
        command=[path+"READ_12.R"],
        mounts=default_mounts,
    )
    

getlist >> [dcrawler1, dcrawler2, dcrawler3, dcrawler4, dcrawler5] >> dsplit
[dcrawler1, dcrawler2, dcrawler3, dcrawler4, dcrawler5] >> insider >> insider_rollup
dsplit >> [drollup1, drollup2] >> drollup3 >> [drollup4, drollup5]  >> rollup_metrics >> [READ_1, READ_2, READ_3, READ_4, READ_5, READ_6, READ_7, READ_8, READ_9, READ_10, READ_11, READ_12] >> bubblerollup
[drollup1, drollup2, drollup3, drollup4, drollup5] >> dailydrollupvalue >> [READ_1, READ_2, READ_3, READ_4, READ_5, READ_6, READ_7, READ_8, READ_9, READ_10, READ_11, READ_12] 
bubblerollup >> hcrawler >> hrollup 
bubblerollup >> mcrawler >> mrollup
bubblerollup >> chart_plotter
