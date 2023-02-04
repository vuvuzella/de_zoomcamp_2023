import pytest
from os.path import isfile, dirname
from pathlib import Path

from etl_project.pipelines import Pipeline
from etl_project.config import DBConfig

output_filename = f"{dirname(__file__)}/../yellow_tripdata_2021-01.csv.gz"
url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{output_filename}"


@pytest.fixture()
def setupTeardown():
    # delete written file
    print("setup...")
    yield
    print("teardown...")
    Path.unlink(Path(output_filename))


def test_fetch_data(setupTeardown):
    config = DBConfig()
    generic_pipeline = Pipeline(url, "db_zoomcamp", config)

    result = generic_pipeline.fetch_data()

    assert isfile(result) == True
