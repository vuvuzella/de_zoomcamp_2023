from datetime import timedelta
from prefect import task
from prefect.tasks import task_input_hash
from etl_project.pipelines import Pipeline


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(p: Pipeline):
    print("Extracting Data...")
    return p.fetch_data()


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def ingest_data(p: Pipeline, filepath: str):
    print("Extracting Data...")
    p.ingest(filepath)
