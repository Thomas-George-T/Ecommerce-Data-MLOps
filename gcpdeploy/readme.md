# Tasks
1. Refactor your respective modules to receive and return dataframe instead of pickles in GCPDeploy/src/
2. Call your respective modules and merge into GCPDeploy/src/datapipeline.py
3. Test datapipeline.py after integrating
4. Complete the steps for models and add it to GCPDeploy/trainer/train.py

# Pipeline
1. Experimental tracking pipeline (MLFLOW)
2. Staging, Production and Archived models (MLFLOW)
3. Model Pipeline:
   * Train the model - The model is trained using K-Means_Clustering function. It takes 3 inputs and gives 2 outputs. The three                        inputs are PCA dataframe, transaction dataframe and number of clusters. The outputs are PCA dataframe                           and transaction dataframe with 'cluster' columns.
   * Save the model -  The model is saved locally and uploaded to GCS.
   * Hyper Parameter Tuning - The model has four hyper-parameters namely init, n_init, number of iterations and number of
                              clusters.
   * Model Analysis - The model is analysed by the Silhouette_Analysis function.
   * Model Efficacy Report and Visuals - The model has the following metrics: Silhouette Score, Calinski Harabasz score and
                                         Davies Bouldin score. 
