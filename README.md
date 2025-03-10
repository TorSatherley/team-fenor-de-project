# Team Fenor Data Engineering Project - ToteSys ETL Pipeline

## Overview

This project is an ETL (Extract, Transform, Load) pipeline that automates data movement from a live PostgreSQL relational database (ToteSys) into a data warehouse optimised for analytics. The pipeline is designed for scalability, reliability and automation using AWS services and Infrastructure as Code.

This project has been an opportunity for our team to showcase our knowledge of Python, SQL, database modelling, AWS, good operational practices and Agile working, amongst other technical and non-technical skills learnt over our time on the Northcoders Data Engineering Bootcamp.

## About

The application of this project can be split into three separate stages: Extract, Transform and Load, or, as shown in the diagram below, the ingestion zone, the processed zone, and the analytics zone.

![alt text](totesys_etl_visual.gif)

### Ingestion Zone

The ingestion zone of the pipeline utilises Python and AWS Lambda to extract data at regular intervals from the TotSys database. The timing of this extraction is managed using Amazon Eventbridge, and the entire process is logged to Cloudwatch, with any failures triggering an email alert. This stage uses Python, SQL and PG8000 to connect to the database, extract the relevant data, and do some minimal processing before storage.

The data is stored in JSONL format in an "ingestion" Amazon S3 bucket acting as a data lake. The data is immutable and will not be changed or destroyed.

### Processed Zone

The processed zone of the pipeline remodels the data into a predefined schema suitable for data warehousing, and stores the data in Parquet format in a "processed" S3 bucket. The data stored in the "processed" bucket is also immutable.

This stage uses Python and Pandas to transform the data stored in the "ingestion" bucket, and is triggered automatically when it detects the completion of a data ingestion job. This transformation includes populating the dimension and fact tables of a single "star" schema in the final data warehouse. The process is again logged and monitored using Cloudwatch.

### Analytics Zone

The analytics zone of the pipeline loads the transformed data from the "processed" bucket into a prepared data warehouse at defined intervals. The process is again logged and monitored using Cloudwatch. The data is now ready to be examined using your BI dashboard of choice, such as AWS Quicksight.

## Prerequisites

Before setting up and running the ETL pipeline, ensure you have the following installed and configured:

- Python 3.13 (recommended latest stable version)

    Check if Python is installed: 
```bash
  python --version
```

If not installed, download it from [python.org](https://www.python.org/downloads/).

- pip (Python package manager)

    Check if pip is installed: 
```bash
  pip --version
```

If missing, install it:
```bash
  python -m ensurepip --default-pip
```

- AWS CLI (for interacting with AWS services):

  Check installation:
```bash
  aws --version
```

If missing, install from [AWS CLI docs](https://aws.amazon.com/cli/).

- AWS credentials (IAM user with necessary permissions):

  Configure credentials using:
```bash
  aws configure
```

- Terraform (for Infrastructure as Code):

  Check installation:
```bash
  terraform --version
```

If missing, install from [Terraform docs](https://developer.hashicorp.com/terraform/install).


## Run Locally

Fork the repo from https://github.com/TorSatherley/team-fenor-de-project

Clone the project:

```bash
  git clone https://github.com/TorSatherley/team-fenor-de-project
```

Go to the project directory:

```bash
  cd team-fenor-de-project
```

Create a new Virtual Environment (recommended):

```bash
  python -m venv venv
```

Activate venv:
```bash
  source venv/bin/activate  # On Linux/macOS
  venv\Scripts\activate  # On Windows
```

Install dependencies

```bash
  pip install -r requirements.txt
```

You can now utilise the Makefile to automate several processes. In the command line run:

```bash
  make run-checks
```
to automate the processes of using:
 - bandit to check for common security issues in Python
 - pip-audit to audit the Python environment for packages with known vulnerabilities
 - black to check for Python PEP 8 compliance
 - pytest to check all unit-tests are running and passing
 - pytest coverage to ensure the test coverage over all Python files exceeds 90%

### Deployment

The ETL pipeline infrastructure is managed using Terraform. Follow these steps to deploy the project onto your own AWS account:

1. Before running any Terraform commands, navigate to the Terraform folder in the project directory and initialise Terraform:
```bash
  cd terraform
  terraform init
```
This downloads the necessary provider plugins and sets up terraform for use.

2. Preview the Infrastructure changes:
```bash
  terraform plan
```
This command will output a summary of what resources will be added, changed, or destroyed without actually making any modifications.

3. Apply the changes:
```bash
  terraform apply
```
This will deploy the infrastructure. Terraform will show a summary of the planned changes and ask for confirmation. Type yes when prompted to proceed. Check on your AWS console that the correct AWS services have been deployed correctly.

4. Destroy the infrastructure (optional):
```bash
  terraform destroy
```
Once you've finished checking out the services deployed on your AWS console, you might want to tear the infrastructure down. Not doing so could cause unwanted costs to your AWS account. You'll be able to deploy the project again using the Terraform Infrastructure as Code anytime you want.

## Roadmap

## Documentation

## Contributors

Alphabetically listed:

- Brendan Corbett
- Connor Creed
- Fabio Greenwood
- Tor Satherley
- Vincent Toor-Azorin



