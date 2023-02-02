# syntax=docker/dockerfile:1
FROM python:3.10-slim-buster AS de_pipeline_week_3

WORKDIR /pipeline

# Optimizations taken from https://github.com/python-poetry/poetry/discussions/1879
ENV \
    # paths
    PATH="${PATH}:/root/.local/bin" \
    VENV_PATH=/opt/pysetup/.venv \
    PYSETUP_PATH=/opt/pysetup \
    \
    # Poetry Optimizations
    POETRY_VERSION=1.3.2 \
    POETRY_NO_INTERACTION=1 \
    POETRY_HOME=/opt/poetry \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    \
    # Python Optimizations
    PYTHONBUFFERED=1 \
    PYTHONDONTWRITERBYTECODE=1

# Copy poetry lock and pyproject files in working directory
COPY poetry.lock ../
COPY pyproject.toml ../

# Install pipx and use pipx to install poetry. Then use poetry to install app dependencies
RUN python3 -m pip install --user pipx \
    && python3 -m pipx ensurepath \
    && pipx install poetry \
    && poetry install --no-root

COPY pipeline/*.py ./

CMD ["poetry", "run", "python", "main.py"]
