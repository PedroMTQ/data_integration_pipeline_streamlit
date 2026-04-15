from datetime import timedelta
from functools import cache

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Variable, dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.bases.operator import chain


@cache
def _load_config() -> dict:
    """Load all pipeline config from Airflow variables and connections.

    Cached so the ~8 DB round-trips happen once per DAG-parse cycle instead of 70+.
    """
    airflow_vars = Variable.get('DIP_AIRFLOW_VARS', deserialize_json=True)
    dip_vars = Variable.get('DIP_CONFIG', deserialize_json=True)
    sensor_patterns = Variable.get('DIP_S3_SENSORS', deserialize_json=True) or {}

    connections = {}
    for conn_id in ('dip_s3_connection', 'dip_pg_connection', 'dip_es_connection'):
        connections[conn_id] = BaseHook.get_connection(conn_id)

    s3 = connections['dip_s3_connection']
    pg = connections['dip_pg_connection']
    es = connections['dip_es_connection']

    connections_config = {
        'S3_ACCESS_KEY': s3.login,
        'S3_SECRET_ACCESS_KEY': s3.password,
        'S3_ENDPOINT_URL': s3.extra_dejson.get('endpoint_url'),
        'POSTGRES_HOST': pg.host,
        'POSTGRES_PORT': pg.port,
        'POSTGRES_USER': pg.login,
        'POSTGRES_PASSWORD': pg.password,
        'POSTGRES_DATABASE': pg.schema,
        'ELASTICSEARCH_URLS': es.host,
        'ELASTICSEARCH_USER': es.login,
        'ELASTICSEARCH_PASSWORD': es.password,
        'ELASTICSEARCH_INDEX_NAME': es.extra_dejson.get('ELASTICSEARCH_INDEX_NAME'),
        'ELASTICSEARCH_INDEX_ALIAS': es.extra_dejson.get('ELASTICSEARCH_INDEX_ALIAS'),
        'ELASTICSEARCH_NUMBER_OF_SHARDS': es.extra_dejson.get('ELASTICSEARCH_NUMBER_OF_SHARDS'),
    }

    env_config = {**connections_config, **dip_vars}

    return {
        'docker_image': airflow_vars['DIP_DOCKER_IMAGE'],
        'max_active_tasks': int(airflow_vars['DIP_MAX_ACTIVE_TASKS']),
        's3_sensor_timeout': int(airflow_vars['DIP_S3_SENSOR_TIMEOUT']),
        's3_sensor_poke': int(airflow_vars['DIP_S3_SENSOR_POKE_INTERVAL']),
        'sensor_patterns': sensor_patterns,
        's3_conn_id': s3.conn_id,
        'env_config': env_config,
        'docker_kwargs': {
            'docker_url': 'tcp://airflow-docker-socket:2375',
            'network_mode': 'data-integration-pipeline-network',
            'auto_remove': 'force',
            'mount_tmp_dir': False,
            'environment': env_config,
            'timeout': 300,
        },
    }


def _docker_kwargs() -> dict:
    cfg = _load_config()
    return {'image': cfg['docker_image'], **cfg['docker_kwargs']}


def create_s3_sensor(task_suffix: str, file_pattern: str) -> S3KeySensor:
    cfg = _load_config()
    return S3KeySensor(
        task_id=f'sensor_{task_suffix}',
        aws_conn_id=cfg['s3_conn_id'],
        bucket_name=cfg['env_config']['DATA_BUCKET'],
        bucket_key=file_pattern,
        timeout=cfg['s3_sensor_timeout'],
        soft_fail=True,
        use_regex=True,
        poke_interval=cfg['s3_sensor_poke'],
        exponential_backoff=True,
    )


def dag_s3_trigger(task_suffix: str):
    """S3 regex gate when ``DIP_S3_SENSORS`` lists a pattern for this key; otherwise a no-op."""
    cfg = _load_config()
    pattern = (cfg['sensor_patterns'].get(task_suffix) or '').strip()
    if pattern:
        return create_s3_sensor(task_suffix, pattern)
    return EmptyOperator(task_id=f'{task_suffix}__s3_trigger_pass')


# ---------------------------------------------------------------------------
# DAGs
# ---------------------------------------------------------------------------


@dag(
    catchup=False,
    tags=['data_integration_pipeline'],
    max_active_tasks=_load_config()['max_active_tasks'],
    schedule=timedelta(minutes=30),
)
def data_integration_pipeline_dag__chunk_bronze():
    trigger = dag_s3_trigger('chunk_bronze')

    @task.docker(**_docker_kwargs())
    def task__get_tasks():
        from data_integration_pipeline.bronze.jobs.chunk_bronze import get_tasks

        results = get_tasks()
        print(f'PRODUCER LOG: Returning {len(results)} items: {results}')
        return results

    @task.docker(**_docker_kwargs())
    def task__process_data(task_data: dict):
        from data_integration_pipeline.bronze.jobs.chunk_bronze import process_task

        return process_task(task_data)

    task_data_list = task__get_tasks()
    processing = task__process_data.expand(task_data=task_data_list)
    chain(trigger, task_data_list, processing)


@dag(
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=['data_integration_pipeline'],
    max_active_tasks=_load_config()['max_active_tasks'],
    max_active_runs=1,
)
def data_integration_pipeline_dag__process_bronze():
    trigger = dag_s3_trigger('process_bronze')

    @task.docker(**_docker_kwargs())
    def task__get_tasks():
        from data_integration_pipeline.bronze.jobs.process_bronze import get_tasks

        results = get_tasks()
        return results

    @task.docker(**_docker_kwargs())
    def task__process_data(task_data: dict):
        from data_integration_pipeline.bronze.jobs.process_bronze import process_task

        return process_task(task_data)

    task_data_list = task__get_tasks()
    processing = task__process_data.expand(task_data=task_data_list)
    chain(trigger, task_data_list, processing)


@dag(
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=['data_integration_pipeline'],
    max_active_tasks=_load_config()['max_active_tasks'],
    max_active_runs=1,
)
def data_integration_pipeline_dag__load_bronze():
    trigger = dag_s3_trigger('load_bronze')

    @task.docker(**_docker_kwargs())
    def task__get_tasks():
        from data_integration_pipeline.bronze.jobs.load_bronze import get_tasks

        results = get_tasks()
        return results

    @task.docker(**_docker_kwargs())
    def task__process_data(task_data: dict):
        from data_integration_pipeline.bronze.jobs.load_bronze import process_task

        return process_task(task_data)

    task_data_list = task__get_tasks()
    processing = task__process_data.expand(task_data=task_data_list)
    chain(trigger, task_data_list, processing)


@dag(
    catchup=False,
    tags=['data_integration_pipeline'],
    max_active_tasks=1,
    max_active_runs=1,
)
def data_integration_pipeline_dag__integrate_silver():

    @task.docker(**_docker_kwargs())
    def task__get_tasks():
        from data_integration_pipeline.silver.jobs.integrate_silver import get_tasks

        results = get_tasks()
        return results

    @task.docker(**_docker_kwargs())
    def task__process_data(task_data: dict):
        from data_integration_pipeline.silver.jobs.integrate_silver import process_task

        return process_task(task_data)

    task_data_list = task__get_tasks()
    processing = task__process_data.expand(task_data=task_data_list)
    chain(task_data_list, processing)


@dag(
    schedule=timedelta(hours=5),
    catchup=False,
    tags=['data_integration_pipeline'],
    max_active_tasks=1,
    max_active_runs=1,
)
def data_integration_pipeline_dag__sync_postgres():

    @task.docker(**_docker_kwargs())
    def task__get_tasks():
        from data_integration_pipeline.gold.jobs.sync_postgres import get_tasks

        return get_tasks()

    @task.docker(**_docker_kwargs())
    def task__process_data(task_data: dict):
        from data_integration_pipeline.gold.jobs.sync_postgres import process_task

        return process_task(task_data)

    task_data_list = task__get_tasks()
    task__process_data.expand(task_data=task_data_list)


@dag(
    schedule=timedelta(hours=5),
    catchup=False,
    tags=['data_integration_pipeline'],
    max_active_tasks=1,
    max_active_runs=1,
)
def data_integration_pipeline_dag__sync_elasticsearch():

    @task.docker(**_docker_kwargs())
    def task__get_tasks():
        from data_integration_pipeline.gold.jobs.sync_elastic_search import get_tasks

        return get_tasks()

    @task.docker(**_docker_kwargs())
    def task__process_data(task_data: dict):
        from data_integration_pipeline.gold.jobs.sync_elastic_search import process_task

        return process_task(task_data)

    task_data_list = task__get_tasks()
    task__process_data.expand(task_data=task_data_list)


@dag(
    schedule=timedelta(hours=5),
    catchup=False,
    tags=['data_integration_pipeline'],
    max_active_tasks=1,
    max_active_runs=1,
)
def data_integration_pipeline_dag__create_pg_report():
    @task.docker(**_docker_kwargs())
    def task__run_job():
        from data_integration_pipeline.gold.jobs.create_pg_report import CreatePostgresReportJob

        CreatePostgresReportJob().run()

    task__run_job()


@dag(
    schedule=timedelta(days=1),
    catchup=False,
    tags=['data_integration_pipeline'],
    max_active_tasks=_load_config()['max_active_tasks'],
    max_active_runs=1,
)
def data_integration_pipeline_dag__optimize_delta():

    @task.docker(**_docker_kwargs())
    def task__get_tasks():
        from data_integration_pipeline.silver.jobs.optimize_delta_tables import get_tasks

        return get_tasks()

    @task.docker(**_docker_kwargs())
    def task__process_data(task_data: dict):
        from data_integration_pipeline.silver.jobs.optimize_delta_tables import process_task

        return process_task(task_data)

    task_data_list = task__get_tasks()
    task__process_data.expand(task_data=task_data_list)


@dag(
    schedule=timedelta(days=1),
    catchup=False,
    tags=['data_integration_pipeline'],
    max_active_tasks=_load_config()['max_active_tasks'],
    max_active_runs=1,
)
def data_integration_pipeline_dag__vacuum_delta():

    @task.docker(**_docker_kwargs())
    def task__get_tasks():
        from data_integration_pipeline.silver.jobs.vacuum_delta_tables import get_tasks

        return get_tasks()

    @task.docker(**_docker_kwargs())
    def task__process_data(task_data: dict):
        from data_integration_pipeline.silver.jobs.vacuum_delta_tables import process_task

        return process_task(task_data)

    task_data_list = task__get_tasks()
    task__process_data.expand(task_data=task_data_list)


### Bronze data processing ###
data_integration_pipeline_dag__chunk_bronze()
data_integration_pipeline_dag__process_bronze()
data_integration_pipeline_dag__load_bronze()

### Generate integrated records data ###
data_integration_pipeline_dag__integrate_silver()

### Sync gold records ###
data_integration_pipeline_dag__sync_postgres()
data_integration_pipeline_dag__sync_elasticsearch()
data_integration_pipeline_dag__create_pg_report()

### Delta cleanup ###
data_integration_pipeline_dag__optimize_delta()
data_integration_pipeline_dag__vacuum_delta()
