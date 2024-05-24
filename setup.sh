# download manifest file for Airflow
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml'

# create necessary directories
mkdir -p ./dags ./logs ./plugins ./config

# add AIRFLOW_UID variable to environment
# echo "AIRFLOW_UID=$(id -u)" > .env

# initialize database
docker compose up airflow-init

##### (Optional) Cleaning-up environment#########

# this should be done because the setup steps above
# is a quick-start and not for production usage

## remove containers not define in Compose file
#docker compose down --volumes --remove-orphans

# remove the entire directory where download the compose file
# (not knowing this yet)
#rm -rf '<DIRECTORY>'

# start Airflow container in detach mode
docker compose up -d



