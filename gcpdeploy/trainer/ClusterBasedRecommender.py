  """
  This module returns a dataframe with top three products which individual customers has not purchased yet based on the top purchased products from their corresponding cluster.
  """
  
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