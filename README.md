# pipeline training

This project contains the following containers:

* postgres: Postgres database for Airflow metadata and a Test database to test.
    * Image: postgres:9.6
    * Database Port: 5432
    * References: https://hub.docker.com/_/postgres

* airflow-webserver: Airflow webserver and Scheduler.
    * Image: airflow-spark:2.2.1_3.1.2
    * Port: 8080

* spark: Spark Master.
    * Image: bitnami/spark:3.1.2
    * Port: 8181
    * References: 
      * https://github.com/bitnami/bitnami-docker-spark
      * https://hub.docker.com/r/bitnami/spark/tags/?page=1&ordering=last_updated

* spark-worker-N: Spark workers. You can add workers copying the containers and changing the container name inside the docker-compose.yml file.
    * Image: bitnami/spark:3.1.2
    * References: 
      * https://github.com/bitnami/bitnami-docker-spark
      * https://hub.docker.com/r/bitnami/spark/tags/?page=1&ordering=last_updated

### Clone project

    $ https://github.com/Katawut-p/pipeline_training.git

### Build airflow Docker

    