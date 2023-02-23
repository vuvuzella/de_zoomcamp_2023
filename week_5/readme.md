TODO:

1. ~~Install spark inrastructure and run as a docker container~~
2. ~~Test infrastructure by creating a pyspark python application and checking it~~
3. Orchestrate a database to use with spark. Or have spark connect to Google Cloud Storage
4. ~~Create week_5 GCS terraform~~
5. Fix spark continer so that a jupyter running on the host can successully register an application without local mode. Prob: if using `"local[*]"` address to reference the master, the application does not show up in spark UI, but the app can query spark jobs. If using `spark:localhost:7077`, the application registers, however the app cannot submit jobs to worker nodes.
