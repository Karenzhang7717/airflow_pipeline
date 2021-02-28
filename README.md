# Airflow Setup

http://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

~~`curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.0.1/docker-compose.yaml'`~~

Don't run above script if using custom build image

clone this repo, and then **cd into the directory where this repo is located**

##### Init

```
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

get images

```
docker-compose up airflow-init
```

compose up

```
docker-compose up --build -d
```

~~`docker-compose up -d`~~ (if custom building)

when done & ready to purge

```
docker-compose down --volumes
docker-compose down --volumes --rmi all
```

[ssh into airflow pod](https://phase2.github.io/devtools/common-tasks/ssh-into-a-container/)

```
docker exec -it airflow-tendie_airflow-webserver_1 /bin/bash
docker exec -it airflow-tendie_airflow-webserver_1 pip install -r requirements.txt
docker exec -it airflow-tendie_airflow-worker_1 pip install -r requirements.txt
```

[deletes everything](https://stackoverflow.com/questions/44785585/docker-how-to-delete-all-local-docker-images)

```
docker system prune -a --volumes
```

restart webserver

```
docker restart airflow-tendie_airflow-webserver_1
```