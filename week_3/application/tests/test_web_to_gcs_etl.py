from etl.web_to_gcs_etl import extract_data
from prefect import flow
from prefect.testing.utilities import prefect_test_harness


@flow
def flow_fixture():
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-05.csv.gz"
    return extract_data(url)


def test_extract():
    with prefect_test_harness():
        result = flow_fixture()
        print(result)
        assert result is not None
