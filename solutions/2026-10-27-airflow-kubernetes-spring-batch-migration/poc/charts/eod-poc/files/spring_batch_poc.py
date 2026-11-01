"""Run the real Java Spring Batch job; its three steps are persisted in PostgreSQL."""
from datetime import timedelta
import pendulum
from airflow.sdk import DAG, Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG(
    "spring_batch_poc", schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Shanghai"),
    catchup=False, max_active_runs=1,
    params={"business_date": Param("2026-11-01", type="string", pattern=r"^\d{4}-\d{2}-\d{2}$"),
            "force_failure": Param(False, type="boolean")},
    tags=["poc", "java", "spring-batch"],
) as dag:
    execute_batch = KubernetesPodOperator(
        task_id="execute_exposure_batch", name="spring-exposure", namespace="batch-poc",
        image="eod-spring-batch:0.1.0", image_pull_policy="Never", in_cluster=True,
        service_account_name="eod-batch", automount_service_account_token=False,
        arguments=["businessDate={{ params.business_date }}", "requestId={{ run_id }}",
                   "forceFailure={{ params.force_failure | lower }}"],
        env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="eod-worker-env"))],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu":"100m","memory":"256Mi"}, limits={"cpu":"1","memory":"768Mi"}),
        pool="eod_compute", deferrable=True, get_logs=True,
        do_xcom_push=False, on_finish_action="delete_succeeded_pod",
        execution_timeout=timedelta(minutes=10), active_deadline_seconds=600,
        log_events_on_failure=True,
    )
