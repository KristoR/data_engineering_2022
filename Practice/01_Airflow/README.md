## Airflow

Use `docker compose up -d` to start up the necessary services.  

Once the services are up and running, you can go to `localhost:8080` to access the Airflow UI.

The login information can be found in the compose yml.

There are many example DAGs which can be tested and investigated. 

From the UI you can trigger DAG executions, monitor running and finished executions, debug task errors and output.

### Custom DAG

Move the `fetch_iss_info.py` file from the current folder to `/dags` folder. In a few moments, it should appear in the Airflow UI.

This DAG has a connection to a Postgres database. Thus, before we can successfully execute the DAG, we need to set up the connection. This can be done in the Airflow UI.

### Connection setup

From the top menu, navigate to Admin -> Connections.

Add a new record (+ sign).

Name the connection_id `airflow_pg`. This will be the reference used by the DAG.

For connection type, choose `Postgres`.

Host / schema / login / password can be found from the compose file.

You can test the connection. If it works, click on Save.

### Reviewing the DAG

Most of the documentation can be viewed from the source code.

Many of the concepts are well explained in the official docs. 

E.g. to read about `trigger_rule` options you can navigate to https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#trigger-rules.


### HOME ASSIGNMENT