## Running the containers:

1. `mkdir -p persistent_store/db_volume`
2. `docker compose build`
3. `docker compose up`

## TODO:

1. notebooks and pipelines should have their own images and Dockerfiles, orchestrated by a docker-compose.yml in the root directory
2. Create github actions for building images that's got changes in respective folders
3. Add dependency groups in pyproject.toml for each of the python projects (notebooks, pipeline, etc...) ala monorepo style
4. Optimize Docker images
5. ~~Generalize the Pipeline class so that different datasets can be used with the same Pipeline class~~
