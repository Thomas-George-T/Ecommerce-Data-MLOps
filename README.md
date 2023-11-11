[![Pytest](https://github.com/Thomas-George-T/Ecommerce-Data-MLOps/actions/workflows/pytest.yml/badge.svg)](https://github.com/Thomas-George-T/Ecommerce-Data-MLOps/actions/workflows/pytest.yml)
# Ecommerce Customer Segmentation & MLOps

<p align="center">  
    <br>
	<a href="#">
	      <img src="https://raw.githubusercontent.com/Thomas-George-T/Thomas-George-T/master/assets/python.svg" alt="Python" title="Python" width ="120" />
        <img height=100 src="https://cdn.svgporn.com/logos/airflow-icon.svg" alt="Airflow" title="Airflow" hspace=20 /> 
        <img height=100 src="https://cdn.svgporn.com/logos/tensorflow.svg" alt="Tensorflow" title="Tensorflow" hspace=20 /> 
        <img height=100 src="https://cdn.svgporn.com/logos/docker-icon.svg" alt="Docker" title="Docker" hspace=20 />
  </a>	
</p>
<br>

# Introduction 
In today's data-driven world, businesses are constantly seeking ways to better understand their customers, anticipate their needs, and tailor their products and services accordingly. One powerful technique that has emerged as a cornerstone of customer-centric strategies is “Customer segmentation”: the process of dividing a diverse customer base into distinct groups based on shared characteristics, that allows organizations to effectively target their marketing efforts, personalize customer experiences, and optimize resource allocation. Clustering, being a fundamental method within the field of unsupervised machine learning, plays a pivotal role in the process of customer segmentation by leveraging the richness of customer data, including behaviors, preferences, purchase history, beyond the geographic demographics to recognize hidden patterns and subsequently group customers who exhibit similar traits or tendencies. As population demographics are proven to strongly follow the Gaussian distribution, a characteristic tendency in an individual could be possessed by other individuals in the relevant cluster, which then may serve as the foundation for tailored marketing campaigns, product recommendations, and service enhancements. By understanding the unique needs and behaviors of each segment, companies can deliver highly personalized experiences, ultimately fostering customer loyalty and driving revenue growth.
In this project of clustering for customer segmentation, we will delve into the essential exploratory data analysis techniques, unsupervised learning methods such as K-means clustering, followed by Cluster Analysis to create targeted profils for customers. The goals of this project comprise data pipeline preparation, ML model training, ML model update, exploring the extent of data and concept drifts (if any), and CI/CD Process demonstration. Thus, this project shall serve as a simulation for real-world application in the latest competitive business landscape. We aim to further apply these clustering algorithms to gain insights into customer behavior, and create a recommendation system as a future scope for lasting impact on customer satisfaction and business success. 

# Dataset Information 
This is a transnational data set which contains all the transactions occurring between 01/12/2010 and 09/12/2011 for a UK-based and registered non-store online retail.The company mainly sells unique all-occasion gifts. Many customers of the company are wholesalers.

## Data Card
- Size: 541909 rows × 8 columns
- Data Types
  
| Variable Name |Role|Type|Description|
|:--------------|:---|:---|:----------|
|InvoiceNo |ID	|Categorical	|a 6-digit integral number uniquely assigned to each transaction. If this code starts with letter 'c', it indicates a cancellation |
|StockCode |ID	|Categorical	|a 5-digit integral number uniquely assigned to each distinct product |
|Description|Feature	|Categorical	|product name |
|Quantity	|Feature	|Integer	|the quantities of each product (item) per transaction |
|InvoiceDate	|Feature	|Date	|the day and time when each transaction was generated |
|UnitPrice	|Feature	|Continuous	|product price per unit |
|CustomerID	|Feature	|Categorical	|a 5-digit integral number uniquely assigned to each customer |
|Country	|Feature	|Categorical	|the name of the country where each customer resides |

## Data Sources 
The data is taken from [UCI repository](https://archive.ics.uci.edu/dataset/352/online+retail)

# Installation
This project uses `Python >= 3.8`. Please ensure that the correct version is installed on your device. This project also works on Windows, Linux and Mac.

# Prerequisities
1. git
2. python>=3.8
3. docker daemon/desktop is running

## User Installation
The steps for User installation are as follows:

1. Clone repository onto the local machine
```
git clone https://github.com/Thomas-George-T/Ecommerce-Data-MLOps.git
```
2. Check python version  >= 3.8
```python
python --version
```
3. Check if you have enough memory
```docker
docker run --rm "debian:bullseye-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'
```

<hr>

**FOR WINDOWS: Create a file called .env in the same folder as `docker-compose.yaml` and set the user as follows:**
```
AIRFLOW_UID=50000
```
**If you get the following error**
```
ValueError: Unable to configure handler 'processor'
```
**Setting the user manually like above fixes it**

<hr>

4. With Docker running, initialize the database. This step only has to be done once.
```docker
docker compose up airflow-init
```
5. Run airflow
```docker
docker-compose up
```
Wait until terminal outputs something similar to

`app-airflow-webserver-1  | 127.0.0.1 - - [17/Feb/2023:09:34:29 +0000] "GET /health HTTP/1.1" 200 141 "-" "curl/7.74.0"`

6. Visit localhost:8080 login with credentials

```
user:airflow2
password:airflow2
```
7. Run the DAG by clicking on the play button on the right side of the window

8. Stop docker containers
```docker
docker compose down
```
# Tools Used for MLOps

- GitHub Actions
- Docker
- Airflow
- DVC
- Google Cloud Platform (GCP)

## GitHub Actions

GitHub Actions workflows are set on push and on pull requests for all branches including the feature** and main branches. On pushing a new commit, the workflow triggers a build involving `pytest` and `pylint`. It should generate test reports in XML formats available as artefacts. 
The workflow will check for test cases available under `test` for the corresponding modules in `src`. By using `pylint`, it runs formatting and code leaks tests ensuring that the codes are readable, prevent potential vulnerabilities and be well documented for future use.

Only on a successful build, the feature branches be merged with the main.

## Docker and Airflow

The `docker-compose.yaml` file contains the code neccessary to run Airflow. Through the use of Docker and containerization, we are able to ship our datapipeline with the required dependencies installed. This makes it platform indepedent, whether it is windows, mac or linux, our data pipeline should run smooth.

## DVC 

Data Versioning Control enables us for versioning of datasets and machine learning models, providing a snapshot of the data used for training and the corresponding code, which in turn allows reproducibility and traceability, ensuring that you can recreate any previous state of your project. DVC stores only meta-information, while the actual data remains in cloud storage or other remote locations.DVC easily integrates with Git, thus it allowed us to use Git repositories for managing code and DVC repositories for managing data and models. This dual-repository approach helps keep the codebase clean and lightweight.

## Google Cloud Platform (GCP)

Our data version control is tracked and hosted on Google Cloud Platform. Google Cloud seamlessly hosts large dataset and its versioning for developing robust ETL pipelines. Multiple Users can access and update the data at once, while inherent support for versioning helps retrieve older versions effortlessly.

GCP allowed us to implement the ETL effieciently while maintaining intermittent files for all the modularized tasks.

1. All one needs to do is initialize a service account to utilize Google Cloud Platform services.
2. Like for every other remote, one needs to download SSH key for remote access.

[Link to view our Files tracked by DVC on Google Cloud Platform](https://console.cloud.google.com/storage/browser/custseg_dvc_store)

![DVC Image](assets/dvc.png)
Pictured: Our data files tracked by DVC in GCP

<hr>

# Data Pipeline

Our data pipeline is modularized right from data ingestion to preprocessing to make our data ready for modeling. It is made sure that every module functions as expected by following Test Driven Development (TDD). This is achieved through enforcing tests for every module. 

We utilize Apache Airflow for our pipeline. We create a DAG with our modules.

![DAG Image](assets/dag.jpg "Airflow DAG")
Pictured: Our Airflow DAG

The following is the explanation of our Data pipeline DAG

# Project Components

The data pipeline in this project consists of several interconnected modules, each performing specific tasks to process the data. We utilize Airflow and Docker to orchestrate and containerize these modules, with each module functioning as a task in the main data pipeline DAG (`datapipeline`).

### 1. Downloading Data:
The first stage involves downloading and unzipping the dataset into the `data` directory. This is achieved through the following modules:
- `download_data.py`: Responsible for downloading the dataset from the specified source.
- `unzip_data.py`: Extracts the contents of the downloaded zip file for further processing.

### 2. Cleaning Data:
In this phase, the dataset undergoes various cleaning and preprocessing steps to ensure data quality and readiness for analysis. The following modules are involved in this process:
- `data_loader.py`: Loads the data from the Excel file and prepares it for cleaning.
- `missing_values_handler.py`: Identifies and handles missing values in the dataset.
- `duplicates_handler.py`: Detects and removes duplicate records to maintain data integrity.
- `transaction_status_handler.py`: Processes the transaction status data, extracting relevant features.
- `anomaly_code_handler.py`: Identifies and corrects anomalies in product codes to ensure data accuracy.
- `cleaning_description.py`: Remove rows where the description contains service related information.
- `removing_zero_unitprice.py`: Removing rows where unit price is zero.
- `outlier_treatment.py`: Identifies and removes outliers.

Each module in the pipeline reads data from an input pickle path, processes it, and outputs the results to an output pickle path. The seamless integration of these modules within Airflow ensures a streamlined and efficient data processing workflow.

### 3. Feature Engineering:
In this step, we perform feature engineering to analyze and modify the features to further improve the training and improve the results and evaluation metrics. The following modules are created for feature engineering:
- `rfm.py`: This module analyzes Recency, Frequency and Monetary methods to know about the value of customers and dividing the base.
- `unique_products.py`: This module groups the values based on unique values of ‘CustomerID’ and orders.
- `customers_behavior.py`: The module shows how the behavioral patterns of customers affect the business based on weekly frequency.
- `geographic_features.py`: This module defines the distribution of the customers' data with respect to regions.
- `cancellation_details.py`: This module shows how cancelling of orders affects the business and the data. It also shows the frequency of cancellation and cancellation			     rate.
- `seasonality.py`: This module analyzes the seasonal trends and how they affect customers and business.

The inputs for these modules are pickle files which are taken as dataframes and outputs are paths to the pickle files which are stores the values from the dataframes which are created after each task.

<hr>

# Contributing / Development Guide

**This is the user guide for developers**

Before developing our code, we should install the required dependencies
```python
pip install -r requirements.txt
```

## Testing
Before pushing code to GitHub, Run the following commands locally to ensure build success. Working on the suggestions given by `Pylint` improves code quality. Ensuring that the test cases are passed by `Pytest` are essential for code reviews and maintaining code quality.

To test for formatting and code leaks, run the following:
```python
pytest --pylint
```

To running the test suites for the modules, run the following:
```python
pytest 
```
## Airflow Dags

### To make your Dags/Append Dag
After your code is built successfully, copy them to `dags/src/`. Write your Python Operator in `airflow.py` under `dags/src/`.
Set dependencies using the `>>` operator.

After this step, we need to edit our `docker-compose.yaml` file

## Docker

Additional: If your code uses additional requirements, please edit `docker-compose.yaml`.
Under Environment or as follows:

```docker
environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    # For backward compatibility, with Airflow <2.3
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
    _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- scikit-learn numpy pandas ipykernel mlflow requests openpyxl requests-mock apache-airflow apache-airflow-providers-celery apache-airflow-providers-common-sql apache-airflow-providers-ftp apache-airflow-providers-http apache-airflow-providers-imap apache-airflow-providers-sqlite Flask Flask-AppBuilder Flask-Babel Flask-Caching Flask-JWT-Extended Flask-Limiter Flask-Login Flask-Session Flask-SQLAlchemy Flask-WTF flower celery }
```
Add your packages to `_PIP_ADDITIONAL_REQUIREMENTS:` in the `docker-compose.yaml` file

Next Step is to initialize the airflow database the first time as shown in `User Installation Step 4`. After this, Continue DAG development till Step 8.

If everything is done right, you should be able to see your module in th DAG. In case of errors, we can access the logs and debug as neccessary.

## DVC

Steps to initialize and track files using DVC

1. Initialize dvc in the parent directory of your local repository.
    ```python
    dvc remote add -d temp /tmp/dvcstore
    ```
2. Set up remote bucket.
    ```python
    dvc remote add -d temp /tmp/dvcstore
    ```
3. Add the location as default to your remote bucket.
    ```python
    dvc remote add -d myremote gs://<mybucket>/<path>
    ```
4. Don't forget to modify your credentials.
    ```python
    dvc remote modify --lab2 credentialpath <YOUR JSON TOKEN>```


<hr>
