import pytest
from os.path import isfile, dirname
from pathlib import Path
from pandas.io.parsers import TextFileReader

from etl_project.pipelines import Pipeline
from etl_project.config import DBConfig

filename = "yellow_tripdata_2021-01.csv.gz"
output_filename = f"{dirname(__file__)}/../etl_project/{filename}"
url = (
    f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{filename}"
)


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

    print(result)

    assert isfile(result) == True

    result = generic_pipeline.fetch_chunks(result)

    assert isinstance(result, TextFileReader) == True
