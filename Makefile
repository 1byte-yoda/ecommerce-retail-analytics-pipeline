.PHONY: down jars_dl up

jars_dl:
	bash docker_services/airflow/download_jar_files.sh

superset_init_db:
	docker exec -it superset_app bash /app/docker/docker-init.sh

airflow_init_uris:
	docker-compose run -it airflow-cli connections add 'spark' --conn-host 'spark://spark' --conn-port '7077' --conn-type 'spark' || echo "Skipping..."

minio_init_access_keys:
	(docker exec -it minio mc alias set myminio http://localhost:9000 admin minioadmin && \
	docker exec -it minio mc admin user svcacct add --access-key "datalake" --secret-key "datalake" myminio admin) \
	|| echo "Skipping..."

trino_init_schemas:
	docker exec -t trino trino --file /etc/scripts/init.sql

up:
	docker-compose up -d

down:
	docker-compose down -v

all:
	make jars_dl
	make up
	echo "Sleeping for 10 seconds to ensure services are up before initializing Database objects"
	sleep 10
	make minio_init_access_keys
	make airflow_init_uris
	make trino_init_schemas
	make superset_init_db