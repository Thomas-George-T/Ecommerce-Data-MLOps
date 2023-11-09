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
This project uses `Python >= 3.10`. Please ensure that the correct version is installed on your device. This project also works on Windows, Linux and Mac.

# Prerequisities
1. git
2. python=3.10
3. docker daemon/desktop is running

## User Installation
The steps for User installation are as follows:

1. Clone repository onto the local machine
2. Install required dependencies
```python
pip install -r requirements.txt
```
3. With Docker running, initialize the database
```docker
docker compose up airflow-init
```
4. Run airflow
```docker
docker-compose up
```
Wait until terminal outputs

`app-airflow-webserver-1  | 127.0.0.1 - - [17/Feb/2023:09:34:29 +0000] "GET /health HTTP/1.1" 200 141 "-" "curl/7.74.0"`

5. Visit localhost:8080 login with credentials

```
user:airflow2
password:airflow2
```
6. Run the DAG by clicking on the play button on the right side of the window

7. Stop docker containers
```docker
docker compose down
```

# GitHub Actions

GitHub Actions workflows are set on push and on pull requests for all branches including the feature** and main branches. On pushing a new commit, the workflow triggers a build involving `pytest` and `pylint`. It should generate test reports in XML formats available as artefacts. 
The workflow will check for test cases available under `test` for the corresponding modules in `src`. By using `pylint`, it runs formatting and code leaks tests ensuring that the codes are readable, prevent potential vulnerabilities and be well documented for future use.

Only on a successful build, the feature branches be merged with the main.

# Contributing / Development

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

After this step, Move onto the `User Installation Step 3`. Continue till Step 6.

If everything is done right, you should be able to see your module in th DAG. In case of errors, we can access the logs and debug as neccessary.