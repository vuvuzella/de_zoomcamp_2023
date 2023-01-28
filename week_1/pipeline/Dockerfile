# syntax=docker/dockerfile:1
FROM python:3.10-slim-buster

WORKDIR /pipeline

# Add pipx bin to path
ENV PATH="${PATH}:/root/.local/bin"
ENV POETRY_NO_INTERACTION=1

COPY ../poetry.lock ../
COPY ../pyproject.toml ../

# datasets to ingest
ADD https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz files/
ADD https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv files/
ADD https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz files/

RUN python3 -m pip install --user pipx \
    && python3 -m pipx ensurepath \
    && pipx install poetry \
    && poetry config virtualenvs.in-project true \
    && poetry install --no-root

COPY pipeline/*.py .

# CMD ["poetry", "run", "python", "pipeline/main.py"]
