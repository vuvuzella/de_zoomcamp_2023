from prefect import task
from etl_project.pipelines import Pipeline


@task(log_prints=True)
def extract_data():
    print("Hellow from extract_data")
