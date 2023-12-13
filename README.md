[![Pytest](https://github.com/Thomas-George-T/Ecommerce-Data-MLOps/actions/workflows/pytest.yml/badge.svg)](https://github.com/Thomas-George-T/Ecommerce-Data-MLOps/actions/workflows/pytest.yml)
# Ecommerce Customer Segmentation & MLOps
[Ashkan Ghanavati](https://github.com/AshyScripts)
[Bardia Mouhebat](https://github.com/baridamm)
[Komal Pardeshi](https://github.com/kokomocha)
[Moheth Muralidharan](https://github.com/Moheth2000)
[Thomas George Thomas](https://github.com/Thomas-George-T)

<p align="center">  
    <br>
	<a href="#">
	      <img src="https://raw.githubusercontent.com/Thomas-George-T/Thomas-George-T/master/assets/python.svg" alt="Python" title="Python" width ="120" />
        <img height=100 src="https://cdn.svgporn.com/logos/airflow-icon.svg" alt="Airflow" title="Airflow" hspace=20 /> 
        <img height=100 src="https://cdn.svgporn.com/logos/tensorflow.svg" alt="Tensorflow" title="Tensorflow" hspace=20 /> 
        <img height=100 src="https://cdn.svgporn.com/logos/docker-icon.svg" alt="Docker" title="Docker" hspace=20 />
        <img height=100 src="https://cdn.svgporn.com/logos/google-cloud.svg" alt="Google Cloud Platform" title="Google Cloud Platform" hspace=20 /> 
        <img height=100 src="https://cdn.svgporn.com/logos/flask.svg" alt="Flask" title="Flask" hspace=20 /> 
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
- MLflow
- TensorFlow
- Flask

## GitHub Actions

GitHub Actions workflows are set on push and on pull requests for all branches including the feature** and main branches. On pushing a new commit, the workflow triggers a build involving `pytest` and `pylint`. It should generate test reports in XML formats available as artefacts. 
The workflow will check for test cases available under `test` for the corresponding modules in `src`. By using `pylint`, it runs formatting and code leaks tests ensuring that the codes are readable, prevent potential vulnerabilities and be well documented for future use.

Only on a successful build, the feature branches be merged with the main.

## Docker and Airflow

The `docker-compose.yaml` file contains the code neccessary to run Airflow. Through the use of Docker and containerization, we are able to ship our datapipeline with the required dependencies installed. This makes it platform indepedent, whether it is windows, mac or linux, our data pipeline should run smooth.

## DVC 

Data Versioning Control enables us for versioning of datasets and machine learning models, providing a snapshot of the data used for training and the corresponding code, which in turn allows reproducibility and traceability, ensuring that you can recreate any previous state of your project. DVC stores only meta-information, while the actual data remains in cloud storage or other remote locations.DVC easily integrates with Git, thus it allowed us to use Git repositories for managing code and DVC repositories for managing data and models. This dual-repository approach helps keep the codebase clean and lightweight.

![How DVC works](assets/dvc-working.png)

## MLFlow

MLflow provided us with a consistent and reproducible environment for experimenting with unsupervised learning algorithms to easily track, compare and save different parameters, metrics, experiments, and even ML models as artifacts for reuse. MLflow seamlessly integrated with frameworks like scikit-learn, TensorFlow which allowed us to tune our model through visualizing the best set of parameters to optimize each of our metrics.

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

## Data Pipeline Components

![Model Pipeline](assets/Data_Pipeline.png "Model Pipeline")

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

# Machine Learning Modeling Pipeline

We have implemented our machine learning pipeline on Google Cloud Platform (GCP). We added our codebase, and we built images using Docker. Subsequently, we pushed the Docker images to the Artifact Registry. We then trained and served our model using Vertex AI.

![ML_Pipeline_Graph_Image](assets/Model_Pipeline.png)

## Machine Learning Pipeline Components
### 1. Trainer
We have a docker file and a python file called train.py that creates the model and stores it into Google Cloud Storage (GCS).
- `ClusterBasedRecommender.py` : It contains K-Means_Clustering algorithm, removing_outliers and hyper parameter tuning.
- `train.py`: Creates the model and saves it on Google Cloud after using the train data from Google Cloud.
- `Dockerfile` : Used to host the training job.
### 2. Serve
It is to serve the K_Means_Clustering on Vertex AI after training.
- `predict.py`: The flask app to predict clusters based on input json data.
- `Dockerfile` : Used to host the serving module.

### 3. Model Pipeline
- `build.py` : Will create a training job using the images from the above trainer and serve in Vertex AI. At the end of the job it wil deploy to the endpoint where it will serve the model.

### 4. Inference
- `inference.py : It will send a json input to the model to predict the results.



## Experimental tracking pipeline (MLFLOW)

For tracking our experimental machine learning pipeline, we use MLflow, Docker, and Python.

We chose the three metrics Davies-Bouldin Inedx(lower the better), Calinski-Harabasz Index(higher the better) and primarily Silhouette score(higher the better) to choose our final model parameters from the plot below.

![MLFlow Parallel Plot Image](assets/KMeans_Parallelplot.png)
Pictured: Parallel Plot for visualizing the parameter-metrics combinations for our model

## Staging, Production and Archived models (MLFLOW)
In managing models for Staging, Production, and Archiving, we rely on MLflow.

## Model Pipeline
   #### Train the model 
   The model is trained using K-Means_Clustering function. It takes 3 inputs and gives 2 outputs. The three inputs are PCA dataframe, transaction dataframe and number of clusters. The outputs are PCA dataframe and transaction dataframe with 'cluster' columns.
   #### Save the model 
   The model is saved locally using save_and_upload_model function and uploaded to GCS.
   #### Hyper Parameter Tuning
   The model has four hyper-parameters namely init, n_init, number of iterations and number of clusters in model_analysis(). Also, we used MLFLOW for checking models with multiple parameters by changing cluster numbers in centroid seeds.
  #### Model Analysis 
  The model is analysed by the Silhouette_Analysis function.


   ![Silhouette_Analysis](assets/Silhouette_analysis.png)

<p align="center">The plot above shows the silhouette score plots for different number of clusters. The closer it is to +1, the better it is</p>
  
  #### Model Efficacy Report and Visuals  
  The model has the following metrics: Silhouette Score, Calinski Harabasz score and Davies Bouldin score. Below are the visuals of clusters formed after PCA and the distribution of customers into clusters.
  
   ![3D_Visualization_of_clusters](assets/3D_Visualization_of_clusters.png)

   <p align="center">The plot above visualises the clusters of customers.</p>
   
   ![Distribution_of_clusters](assets/Distribtion_customers.png)

   <p align="center">The plot above visualises the distribution of customers into clusters.</p>
 

<hr> 

# Cost Analysis

Breakdown of the costs associated with the Machine Learning pipeline on Google Cloud Platform (GCP) hosted on US East1 Region.

## Initial Cost Analysis

Model Training using Vertex AI: $3.58

Deploying Model: $1.75

Total Training and Deployment Cost: $5.33

## Serving Analysis

Daily Online Prediction for Model Serving: $6.63

Weekly serving cost: $46.41

Monthly serving cost: $185.64

Yearly serving cost: $2,423.72

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

## MLFlow

Most important declarations in the code:
1. Set your tracking uri for MLFlow.
    ```python
    mlflow.set_tracking_uri("http://127.0.0.1:5001")
    ```
2. Setting the base level for logging; only warnings and above (error,critical) shall be logged.
    ```python
    logging.basicConfig(level=logging.WARN)
    ```

3. Set up the logger.
    ```python
    logger = logging.getLogger(__name__)
    ```

4. Additionally, you may or may not choose to ignore warnings.
    ```python
    warnings.filterwarnings("ignore")
    ```
<hr>

# Model Insights

## Segmentation Clusters

### Cluster 0
Profile: Recurrent High Spenders with High Cancellations

- Consumers in this cluster buy a wide range of unusual goods and have very high overall spending power.
- They do a lot of transactions, but they also cancel a lot and with high frequency.
- These clients typically shop early in the day and have very short average time intervals between transactions (low Hour value).
- Their high level of monthly variability suggests that, in comparison to other clusters, their spending patterns may be less predictable.
- They exhibit a low spending tendency in spite of their high expenditure, which raises the possibility that their high spending levels will eventually decline.

![Cluster 0](data/plots/Cluster0.jpeg)

### Cluster 1
Profile:  Intermittent Big Spenders with a High Spending Trends
- The moderate spending levels of the customers in this cluster are accompanied by infrequent transactions, as seen by the high Days_Since_Last_Purchase and Average_Days_Between_Purchases values.
- Their expenditure trend is really high, suggesting that they have been spending more money over time.
- These clients, who are primarily from the UK, prefer to purchase late in the day, as seen by the high Hour value.
- They typically cancel a modest amount of transactions, with a moderate frequency and rate of cancellations.
- Their comparatively high average transaction value indicates that people typically make large purchases when they go shopping.

![Cluster 1](data/plots/Cluster1.jpeg)

### Cluster 2
Profile: Sporadic Shoppers with a Proclivity for Weekend Shopping

- Consumers in this cluster typically make fewer purchases and spend less money overall.
- The very high Day_of_Week number suggests that they have a slight inclination to shop on the weekends.
- Their monthly spending variation is low (low Monthly_Spending_Std), and their spending trend is generally constant but on the lower side.
- These customers have a low cancellation frequency and rate, indicating that they have not engaged in numerous cancellations.
- When they do shop, they typically spend less each transaction, as seen by the lower average transaction value.


![Cluster 1](data/plots/Cluster2.jpeg)

## Customer RFM Trends based on Clusters

![Customer Trends Histogram](data/plots/histogram_analysis.png)
