# Machine Learning Modelling Pipeline
## 1. Experimental tracking pipeline (MLFLOW)
## 2. Staging, Production and Archived models (MLFLOW)
## 3. Model Pipeline:
   ### Train the model 
   The model is trained using K-Means_Clustering function. It takes 3 inputs and gives 2 outputs. The three inputs are PCA dataframe, transaction dataframe and number of clusters. The outputs are PCA dataframe   and transaction dataframe with 'cluster' columns.
   ### Save the model 
   The model is saved locally using save_and_upload_model function and uploaded to GCS.
   ### Hyper Parameter Tuning
   The model has four hyper-parameters namely init, n_init, number of iterations and number of clusters in model_analysis(). Also, we used MLFLOW for checking models with multiple parameters by changing cluster numbers in centroid seeds.
  ### Model Analysis 
  The model is analysed by the Silhouette_Analysis function.
  ### Model Efficacy Report and Visuals  
  The model has the following metrics: Silhouette Score, Calinski Harabasz score and Davies Bouldin score and plots were visualized.
