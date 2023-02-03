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
    'start_date': datetime(1980, 1, 1).replace(tzinfo=pendulum.timezone('Asia/Seoul'))
}

with DAG(
    dag_id='kra-horseinfohi-v4', 
    schedule_interval = '0 0 1 * *', 
    default_args=default_args,
    max_active_runs=10,
    ) as dag:

    crawler = KubernetesPodOperator(
        name="kra-horseinfohi-crawler",
        image="docker.items.com/korea-racing-authority",
        task_id="kra-horseinfohi-crawler",
        namespace='default',
        do_xcom_push=True,
        cmds=['python3', 'kra_horseinfohi_crawler.py', '--search-date', '{{ next_ds }}', '--ignore-nodata-error'],
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
        name="kra-horseinfohi-parser",
        image="docker.items.com/korea-racing-authority",
        task_id="kra-horseinfohi-parser",
        namespace='default',
        do_xcom_push=True,
        volumes=[volume],
        env_vars=env_vars,
        volume_mounts=[volume_mount],
        cmds=['/lib/spark/bin/spark-submit', 'kra_horseinfohi_parser.py'],
        # cmds=['tail', '-f', '/dev/null'],
        get_logs=True,
        is_delete_operator_pod=False,
    )
    # crawler
    crawler >> parser
