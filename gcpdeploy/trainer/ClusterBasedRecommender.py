  """
  This module returns a dataframe with top three products which individual customers has not purchased yet based on the top purchased products from their corresponding cluster.
  """
  
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
