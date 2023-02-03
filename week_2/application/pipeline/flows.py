from prefect.tasks import task
from prefect.flows import flow
from pipeline.pipelines import Pipeline


@task(log_prints=True)
def extract_data(p: Pipeline):
    ...


def transform_data(p: Pipeline):
    ...


def write_data(p: Pipeline):
    ...
