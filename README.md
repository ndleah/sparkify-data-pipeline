![Star Badge](https://img.shields.io/static/v1?label=%F0%9F%8C%9F&message=If%20Useful&style=style=flat&color=BC4E99)
![Open Source Love](https://badges.frapsoft.com/os/v1/open-source.svg?v=103)
# Sparkify - Data Pipelines <img src="https://static.vecteezy.com/system/resources/previews/009/726/165/original/pixel-art-isometric-islands-in-the-sky-with-trees-bridge-lake-and-fence-8bit-game-scenario-vector.jpg" align="right" width="150" />

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

![Airflow DAG](assets/dag.png)

## Dataset
**Note** that the actual data (in *JSON*) used in this project is a subset of original dataset preprocessed by the course. The provided data 
resides in AWS S3 (publically available).
1. Song data from [Million Song Dataset](http://millionsongdataset.com/)
2. User activity data from [Event Simulator](https://github.com/Interana/eventsim) based on [Million Song Dataset](http://millionsongdataset.com/)

## Star Schema Design
![Airflow DAG](assets/ERD.png)
## Requirements

* Install [Python3](https://www.python.org/downloads/)
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* [AWS](https://aws.amazon.com/) account and [Redshift](https://aws.amazon.com/redshift/) cluster 

### Clone repository to local machine
```
git clone https://github.com/ndleah/sparkify-data-pipeline.git
```

### Change directory to local repository
```
cd sparkify-data-pipeline
```
#### Create python virtual environment
```
python3 -m venv venv             # create virtualenv
source venv/bin/activate         # activate virtualenv
pip install -r requirements.txt  # install requirements
```

#### Start Airflow container
Everything is configured in the docker-compose.yml file.
If you are satisfied with the default configurations you can just start the containers.
```
docker-compose up
```

#### Visit the Airflow UI
Go to http://localhost:8080

Username: airflow
Password: airflow

### Connect Airflow to AWS

1. Click on the Admin tab and select Connections.
![Admin tab](https://video.udacity-data.com/topher/2019/February/5c5aaca1_admin-connections/admin-connections.png)

2. Under Connections, select Create.

3. On the create connection page, enter the following values:
- Conn Id: Enter aws_credentials.
- Conn Type: Enter Amazon Web Services.
- Login: Enter your Access key ID from the IAM User credentials.
- Password: Enter your Secret access key from the IAM User credentials.
![aws_credentials](https://video.udacity-data.com/topher/2019/February/5c5aaefe_connection-aws-credentials/connection-aws-credentials.png)
Once you've entered these values, select Save and Add Another.

4. On the next create connection page, enter the following values:
- Conn Id: Enter redshift.
- Conn Type: Enter Postgres.
- Host: Enter the endpoint of your Redshift cluster, excluding the port at the end.
- Schema: Enter dev. This is the Redshift database you want to connect to.
- Login: Enter awsuser.
- Password: Enter the password you created when launching your Redshift cluster.
- Port: Enter 5439.
![redshift](https://video.udacity-data.com/topher/2019/February/5c5aaf07_connection-redshift/connection-redshift.png)
Once you've entered these values, select Save.

### Start the DAG
Start the DAG by switching it state from OFF to ON.

Refresh the page and click on the s3_to_redshift_dag to view the current state.

The whole pipeline should take around 10 minutes to complete.

