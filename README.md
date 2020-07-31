# eJP XML Airflow Data Pipeline
This repository consists of a generic data pipeline that is used to ETL eJP XML dumps in S3 buckets.
Being generic, it needs to be configured for its data source, data sink, and transformations.
The sample configuration for this data pipeline can be found in `sample_data_config` directory of this project

## Running The Pipeline Locally
This repo is designed to run in as a containerized application in the development environment.
To run this locally, review the `docker-compose.dev.override.yml` and `docker-compose.yaml` files, and ensure that the different credentials files required by different data pipelines are correctly provided.
Following are the credentials that you may need to provide
- GCP's service account json key (mandatory for all data pipelines)
- AWS credentials

To run the application locally:

    make dev-env

To run the whole test on the application:
       
    make dev-end2end-test
 
To run tests excluding the end to end tests:

    make dev-test-exclude-e2e
 
To set up the development environment:

    make dev-install
 
 
## Project Folder/Package Organisation

- `dags` package contains the airflow's data pipeline dag. 
- `ejp_xml_pipeline` package consist of the packages and libraries and functions needed to run the pipeline in `dags` package.
- `tests` contains the tests run on this implementation. These include these types
  - unit tests
  - end to end tests
  - dag validation tests
- `sample_data_config` folder contains the sample configurations for the data pipeline
 
 
 ## CI/CD
 
 This runs on Jenkins and follows the standard approaches used by the `eLife Data Team` for CI/CD.
 Note that as part of the CI/CD, another Jenkins pipeline is always triggered whenever there is a commit to the develop branch. The latest commit reference to a `develop` branch is passed on as a parameter to this Jenkins pipeline to be triggered, and this is used to update the [repo-list.json file](https://github.com/elifesciences/data-hub-airflow-image/blob/develop/repo-list.json) in another repository
