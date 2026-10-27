"""EOD orchestration POC. Domain logic is simulated; replace worker image for Java."""
import os
from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

NAMESPACE = os.getenv("EOD_NAMESPACE", "batch-poc")
IMAGE = os.getenv("EOD_WORKER_IMAGE", "apache/airflow:3.2.2")
RUN_PREFIX = "{{ params.business_date }}/{{ params.input_version }}"

with DAG(
    dag_id="eod_exposure_poc",
    description="Manifest → ingest → tree snapshot → exposure → reconcile",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Shanghai"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(minutes=30),
    default_args={"owner": "batch-team", "retries": 1, "retry_delay": timedelta(seconds=20)},
    params={
        "business_date": Param("2026-10-26", type="string", pattern=r"^\d{4}-\d{2}-\d{2}$"),
        "input_version": Param("demo-v1", type="string", pattern=r"^[A-Za-z0-9_-]{1,40}$"),
        "config_version": Param("demo-config-1", type="string", enum=["demo-config-1"]),
    },
    tags=["poc", "eod", "kubernetes", "simulated-domains"],
) as dag:
    wait_manifest = S3KeySensor(
        task_id="wait_manifest",
        bucket_name="eod-input",
        bucket_key=f"{RUN_PREFIX}/manifest.json",
        aws_conn_id="eod_s3",
        deferrable=True,
        poke_interval=10,
        timeout=600,
        retries=0,
    )

    def domain_task(stage):
        return KubernetesPodOperator(
            task_id=stage,
            name=f"eod-{stage}",
            namespace=NAMESPACE,
            image=IMAGE,
            image_pull_policy="IfNotPresent",
            in_cluster=True,
            service_account_name="eod-batch",
            automount_service_account_token=False,
            cmds=["python", "/opt/eod/worker.py"],
            arguments=[stage],
            env_vars={
                "BUSINESS_DATE": "{{ params.business_date }}",
                "INPUT_VERSION": "{{ params.input_version }}",
                "CONFIG_VERSION": "{{ params.config_version }}",
                "AIRFLOW_RUN_ID": "{{ run_id }}",
                "S3_ENDPOINT": f"http://eod-minio.{NAMESPACE}.svc.cluster.local:9000",
                "AWS_DEFAULT_REGION": "us-east-1",
                "AWS_EC2_METADATA_DISABLED": "true",
                "PGHOST": f"eod-postgres.{NAMESPACE}.svc.cluster.local",
                "PGDATABASE": "airflow",
                "PGUSER": "airflow",
            },
            env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="eod-worker-env"))],
            volumes=[
                k8s.V1Volume(name="worker", config_map=k8s.V1ConfigMapVolumeSource(name="eod-worker")),
                k8s.V1Volume(name="config", config_map=k8s.V1ConfigMapVolumeSource(name="eod-domain-config")),
            ],
            volume_mounts=[
                k8s.V1VolumeMount(name="worker", mount_path="/opt/eod", read_only=True),
                k8s.V1VolumeMount(name="config", mount_path="/opt/eod-config", read_only=True),
            ],
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "100m", "memory": "128Mi"},
                limits={"cpu": "500m", "memory": "512Mi"},
            ),
            container_security_context=k8s.V1SecurityContext(
                allow_privilege_escalation=False,
                capabilities=k8s.V1Capabilities(drop=["ALL"]),
            ),
            labels={"app": "eod-domain", "domain": stage},
            pool="eod_compute",
            pool_slots=1,
            deferrable=True,
            poll_interval=5,
            startup_timeout_seconds=300,
            execution_timeout=timedelta(minutes=10),
            active_deadline_seconds=600,
            get_logs=True,
            log_events_on_failure=True,
            do_xcom_push=False,
            on_finish_action="delete_succeeded_pod",
        )

    ingest = domain_task("ingest")
    tree = domain_task("tree")
    exposure = domain_task("exposure")
    reconcile = domain_task("reconcile")
    wait_manifest >> ingest >> tree >> exposure >> reconcile
