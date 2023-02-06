1. Using the etl_web_to_gcs.py flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.
How many rows does that dataset have?

- Answer: 447,770

2. Cron is a common scheduling specification for workflows.
Using the flow in etl_web_to_gcs.py, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- Answer: 0 5 1 * *

3. How many rows did your flow code process?

- Answer: 126348750 + 140985810 = 26733450

4. How many rows were processed by the script?

- Answer: 

5. How many rows were processed by the script?

- Amswer: 

6. Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- Answer:
