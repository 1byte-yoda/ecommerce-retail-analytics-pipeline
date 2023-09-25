# 2LUX - DATA ANALYTICS PLATFORM

## High Level Data Architecture
![archi](images/2lux_high_level_archi.gif)

## Star Schema
![erd](images/2lux_erd.png)

## Software Requirements
* [Docker Desktop Windows](https://hub.docker.com/editions/community/docker-ce-desktop-windows)
* [Docker Desktop Mac](https://docs.docker.com/desktop/install/mac-install)
* [Docker Linux](https://docs.docker.com/install/linux/docker-ce/ubuntu/)
* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
   
## IMPORTANT
* The first time the environment is started, all images will be downloaded to the local machine.
* To run the entire environment, the server/host must have at least 16GB of memory.


### STARTING THE ENVIRONMENT

#### In a terminal/DOS/PowerShell, clone the project to github.
      git clone https://github.com/1byte-yoda/two-lux.git


#### When you clone the repository, the two-lux directory will be created on your local machine.

## EXAMPLES OF HOW TO START THE ENVIRONMENT

   *On Windows open PowerShell, on Linux/Mac a terminal and access the `two-lux` directory*

### To Spin-up all Containers
      make all
This will download the images and Spark jars needed, and then initialize all credentials and database objects required.

### To Stop all Containers
      make down

## TROUBLESHOOTING PROBLEMS

### Checking Running Containers
      docker ps

### Access Container Logs
      docker container logs <name of the container> 

## Web UI Access URL for each Framework
 
* Minio *http://localhost:9051*
* Jupyter Spark *http://localhost:8888*
* Airflow *http://localhost:8280*
* Superset *http://localhost:8088*
* Trino *http://localhost:8080*

## Login Credentials
   ##### Airflow
    Username: airflow
    Password: airflow

   ##### Superset
    Username: admin
    Password: admin
   
   ##### Minio
    Username: admin
    Password: minioadmin

   ##### Trino
    Username: admin

   ##### Postgres
    Username: admin
    Password: admin

   ##### MySQL
    Username: admin
    Password: admin


## Official Documentation for each Container Used

* [Trino](https://trino.io/docs/current/installation/containers.html)
* [Superset](https://superset.apache.org/docs/installation/installing-superset-using-docker-compose/)
* [DeltaLake](https://delta.io/)
* [Minio](https://min.io/docs/minio/container/operations/installation.html)
* [Postgres](https://github.com/docker-library/postgres)
* [Jupyter Spark](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/specifics.html)
* [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
