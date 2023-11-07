[![Pytest](https://github.com/Thomas-George-T/Ecommerce-Data-MLOps/actions/workflows/pytest.yml/badge.svg)](https://github.com/Thomas-George-T/Ecommerce-Data-MLOps/actions/workflows/pytest.yml)
# Ecommerce Customer Segmentation & MLOps
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
1. Clone repository onto the local machine
2. Install the required dependencies
```python
pip install -r requirements.txt
```

# GitHub Actions

Added GitHub Actions on push for all branches including the feature** and main branches. On pushing a new commit, triggers a build involving pytest and pylint generating test reports as artefacts. 
This workflow will check for test cases available under `test` for the corresponding codes in `src`. It also runs a formatting and code leaks tests ensuring that the codes are readable and well documented for future use.
Only on a successful build, the feature branches can be merged with the main.

Before pushing code to GitHub, Run the following commands locally to ensure build success

```python
pytest --pylint
pytest 
```
