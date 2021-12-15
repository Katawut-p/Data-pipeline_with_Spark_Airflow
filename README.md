# Pipeline training

This project contains the following containers:

* postgres: Postgres database for Airflow metadata and a Test database to test.
    * Image: postgres:13
    * Database Port: 5432
    * References: https://hub.docker.com/_/postgres
<br/><br/>
* airflow-webserver: Airflow webserver and Scheduler.
    * Image: airflow-spark:2.2.1_3.1.2
    * Port: 8080
<br/><br/>
* spark: Spark Master.
    * Image: bitnami/spark:3.1.2
    * Port: 8181
    * References: 
      * https://github.com/bitnami/bitnami-docker-spark
      * https://hub.docker.com/r/bitnami/spark/tags/?page=1&ordering=last_updated
<br/><br/>
* spark-worker-N: Spark workers. You can add workers copying the containers and changing the container name inside the docker-compose.yml file.
    * Image: bitnami/spark:3.1.2
    * References: 
      * https://github.com/bitnami/bitnami-docker-spark
      * https://hub.docker.com/r/bitnami/spark/tags/?page=1&ordering=last_updated

## Setup
### Clone project

    $ https://github.com/Katawut-p/pipeline_training.git

### Build airflow-spark

    execute this command
    docker build --rm --force-rm -t airflow-spark:2.2.1_3.1.2 .

### Start containers

    $ docker-compose up

If you want to run in background:

    $ docker-compose up -d

Note: when running the docker-compose for the first time, the images `postgres:13` and `bitnami/spark:3.1.2` will be downloaded before the containers started.

### Check access service

Airflow: http://localhost:8080

Spark Master: http://localhost:8181

PostgreSql - Database Test:

* Server: localhost:5432
* Database: test
* User: test
* Password: postgres

Postgres - Database airflow:

* Server: localhost:5432
* Database: airflow
* User: airflow
* Password: airflow

## setting spark_default to testing dags

1. go to admin > connections
2. edit the spark_default connection
    - inserting `spark://spark` in Host field and Port `7077`

Note: if not found spark_default connection just create connection follow setting
   - Connection Id: `spark_default`
   - Connection Type: select `spark`
   - Host: `spark://spark`
   - Port: `7077`
   - Extra: `{"queue": "root.default"}`
