# syntax=docker/dockerfile:1
FROM python:3.10-slim-buster

WORKDIR /pipeline

RUN python3 -m pip install --user pipx
RUN python3 -m pipx ensurepath

# Add pipx bin to path
ENV PATH="${PATH}:/root/.local/bin"

RUN pipx install poetry

ENV POETRY_NO_INTERACTION=1

RUN poetry config virtualenvs.in-project true

COPY ../poetry.lock ../
COPY ../pyproject.toml ../

RUN poetry install --no-root

COPY pipeline/*.py .

# CMD ["poetry", "run", "python", "pipeline/main.py"]
