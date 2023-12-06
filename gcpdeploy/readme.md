# Machine Learning Modelling Pipeline
## Experimental tracking pipeline (MLFLOW)
## Staging, Production and Archived models (MLFLOW)
## Model Pipeline
   #### Train the model 
   The model is trained using K-Means_Clustering function. It takes 3 inputs and gives 2 outputs. The three inputs are PCA dataframe, transaction dataframe and number of clusters. The outputs are PCA dataframe and transaction dataframe with 'cluster' columns.
   #### Save the model 
   The model is saved locally using save_and_upload_model function and uploaded to GCS.
   #### Hyper Parameter Tuning
   The model has four hyper-parameters namely init, n_init, number of iterations and number of clusters in model_analysis(). Also, we used MLFLOW for checking models with multiple parameters by changing cluster numbers in centroid seeds.
  #### Model Analysis 
  The model is analysed by the Silhouette_Analysis function.


   ![Silhouette_Analysis](../assets/Silhouette_analysis.png)

<p align="center">The plot above shows the silhouette score plots for different number of clusters. The closer it is to +1, the better it is</p>
  
  #### Model Efficacy Report and Visuals  
  The model has the following metrics: Silhouette Score, Calinski Harabasz score and Davies Bouldin score. Below are the visuals of clusters formed after PCA and the distribution of customers into clusters.
  
   ![3D_Visualization_of_clusters](../assets/3D_Visualization_of_clusters.png)

   <p align="center">The plot above visualises the clusters of customers.</p>
   
   ![Distribution_of_clusters](../assets/Distribtion_customers.png)

   <p align="center">The plot above visualises the distribution of customers into clusters.</p>
 
