"""
This module returns a dataframe with top three products which individual customers has not purchased yet based on the top purchased products from their corresponding cluster.
"""

import pandas as pd

# Removing outliers from transactions dataframe
def remove_outliers(df, outliers_data):
  """
  Removes transactions related to outlier customers from the main dataframe.

  Parameters:
  df (DataFrame): The main transaction dataframe.
  outliers_data (DataFrame): Dataframe containing outlier customer IDs.

  Returns:
  DataFrame: A filtered dataframe excluding outlier customer transactions.
  """
  # Extract the CustomerIDs of the outliers and convert them to float for consistency
  outlier_customer_ids = outliers_data['CustomerID'].astype('float').unique()

  # Filter the main dataframe to exclude transactions from outlier customers
  df_filtered = df[~df['CustomerID'].isin(outlier_customer_ids)]

  return df_filtered

# Merging clusters to the transactions dataframe
def merge_customer_transactions(df, customer_data_cleaned):
  """
  Merges the transaction data with customer data to include cluster information
  for each transaction.

  Parameters:
  df (DataFrame): The transaction dataframe, filtered to exclude outliers.
  customer_data_cleaned (DataFrame): Customer data with clustering information.

  Returns:
  DataFrame: A merged dataframe including both transaction and cluster data.
  """
  # Ensure consistent data type for CustomerID across both dataframes before merging
  df = df.copy()
  df['CustomerID'] = df['CustomerID'].astype('float')

  customer_data_cleaned['CustomerID'] = customer_data_cleaned['CustomerID'].astype('float')

  # Merge the transaction data with the customer data
  merged_data = df.merge(customer_data_cleaned[['CustomerID', 'cluster']], on='CustomerID', how='inner')

  return merged_data

# Identify top 10 products for each cluster
def identify_top_products(merged_data):
  """
  Identifies the top 10 best-selling products in each customer cluster.

  Parameters:
  merged_data (DataFrame): The dataframe from merging transaction data with 
                            customer cluster information.

  Returns:
  DataFrame: A dataframe of the top 10 products for each cluster.
  """
  # Group by cluster, StockCode, and Description and sum the quantities
  grouped_data = merged_data.groupby(['cluster', 'StockCode', 'Description'])['Quantity'].sum().reset_index()

  # Sort the products in each cluster by the total quantity sold in descending order
  sorted_grouped_data = grouped_data.sort_values(by=['cluster', 'Quantity'], ascending=[True, False])

  # Select the top 10 products for each cluster
  top_products_per_cluster = sorted_grouped_data.groupby('cluster').head(10)

  return top_products_per_cluster

# create a record of products purchased by each customer in each cluster
def record_customer_purchases(merged_data):
  """
  Creates a record of the quantities of each product purchased by each customer 
  in each cluster.

  Parameters:
  merged_data (DataFrame): The dataframe from merging transaction data with 
                            customer cluster information.

  Returns:
  DataFrame: A dataframe detailing customer purchases in each cluster.
  """
  # Group by CustomerID, cluster, and StockCode and sum the quantities
  customer_purchases = merged_data.groupby(['CustomerID', 'cluster', 'StockCode'])['Quantity'].sum().reset_index()

  return customer_purchases

# generate product recommendations for each customer
def create_recommendations(customer_data, top_products_per_cluster, customer_purchases):
  """
  Generates product recommendations for each customer based on the top products 
  in their cluster and their purchase history.

  Parameters:
  customer_data (DataFrame): Cleaned customer data with cluster information.
  top_products_per_cluster (DataFrame): Dataframe of top 10 products for each cluster.
  customer_purchases (DataFrame): Dataframe detailing products purchased by each customer.

  Returns:
  list: A list of recommendations for each customer.
  """
  recommendations = []

  for cluster in top_products_per_cluster['cluster'].unique():
      # Retrieve top products for the current cluster
      top_products = top_products_per_cluster[top_products_per_cluster['cluster'] == cluster]
      customers_in_cluster = customer_data[customer_data['cluster'] == cluster]['CustomerID']
      
      for customer in customers_in_cluster:
          # Identify products already purchased by the customer
          customer_purchased_products = customer_purchases[
              (customer_purchases['CustomerID'] == customer) & 
              (customer_purchases['cluster'] == cluster)
          ]['StockCode'].tolist()
          
          # Find top products in the best-selling list that the customer hasn't purchased yet
          top_products_not_purchased = top_products[~top_products['StockCode'].isin(customer_purchased_products)]
          top_3_products_not_purchased = top_products_not_purchased.head(3)
          
          # Append the recommendations to the list
          recommended_items = top_3_products_not_purchased[['StockCode', 'Description']].values.flatten().tolist()
          recommendations.append([customer, cluster] + recommended_items)

  return recommendations

# orchestrate the recommendation generation process by utilizing the previously defined functions
def generate_recommendations(df, outliers_data, customer_data_cleaned):
  """
  Generates product recommendations for each customer based on clustering.

  Parameters:
  df (DataFrame): Transaction data.
  outliers_data (DataFrame): Data of outlier customers.
  customer_data_cleaned (DataFrame): Cleaned customer data with clustering info.

  Returns:
  DataFrame: Recommendations for each customer.
  """
  # Step 1: Remove outliers from the transaction data
  df_filtered = remove_outliers(df, outliers_data)

  # Step 2: Merge the transaction data with customer data to get cluster information
  merged_data = merge_customer_transactions(df_filtered, customer_data_cleaned)

  # Step 3: Identify top-selling products in each cluster
  top_products_per_cluster = identify_top_products(merged_data)

  # Step 4: Record the products purchased by each customer
  customer_purchases = record_customer_purchases(merged_data)

  # Step 5: Generate personalized product recommendations
  recommendations_list = create_recommendations(customer_data_cleaned, top_products_per_cluster, customer_purchases)

  # Step 6: Convert the recommendations list to a DataFrame
  recommendations_columns = ['CustomerID', 'cluster', 'Rec1_StockCode', 'Rec1_Description',
                              'Rec2_StockCode', 'Rec2_Description', 'Rec3_StockCode', 'Rec3_Description']
  recommendations_df = pd.DataFrame(recommendations_list, columns=recommendations_columns)

  return recommendations_df

