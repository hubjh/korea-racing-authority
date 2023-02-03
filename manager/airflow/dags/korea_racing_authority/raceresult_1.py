import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
import pendulum
from kubernetes.client import models as k8s

default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'start_date': datetime(1985, 1, 1).replace(tzinfo=pendulum.timezone('Asia/Seoul'))
}

with DAG(
    dag_id='kra-raceresult-1-v4', 
    schedule_interval = '0 0 1 * *', 
    default_args=default_args,
    max_active_runs=10,
    ) as dag:

    crawler = KubernetesPodOperator(
        name="kra-raceresult-1-crawler",
        image="docker.items.com/korea-racing-authority",
        task_id="kra-raceresult-1-crawler",
        namespace='default',
        do_xcom_push=True,
        cmds=['python3', 'kra_raceresult_1_crawler.py', '--search-date', '{{ next_ds }}', '--ignore-nodata-error'],
        get_logs=True,
        is_delete_operator_pod=False,
    )


    volume_mount = k8s.V1VolumeMount(name='spark-home',
                            mount_path='/lib/spark',
                            sub_path=None,
                            read_only=False)

    volume = k8s.V1Volume(name="spark-home", host_path=k8s.V1HostPathVolumeSource(path="/lib/spark"))
    env_vars = [k8s.V1EnvVar(name='SPARK_HOME', value='/lib/spark')]



    parser = KubernetesPodOperator(
        name="kra-raceresult-1-parser",
        image="docker.items.com/korea-racing-authority",
        task_id="kra-raceresult-1-parser",
        namespace='default',
        do_xcom_push=True,
        volumes=[volume],
        env_vars=env_vars,
        volume_mounts=[volume_mount],
        cmds=['/lib/spark/bin/spark-submit', 'kra_raceresult_1_parser.py'],
        # cmds=['tail', '-f', '/dev/null'],
        get_logs=True,
        is_delete_operator_pod=False,
    )
    # crawler
    crawler >> parser
