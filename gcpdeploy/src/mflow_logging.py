"""
Mlflow module for logging artifacts.
"""
import os
import logging
import warnings
import json
from datetime import datetime

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.metrics import davies_bouldin_score
from sklearn.metrics import calinski_harabasz_score
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

#Loading file
PAR_DIRECTORY = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
__INGESTPATH__ = os.path.join(PAR_DIRECTORY,"data", "processed", "pca_output.parquet")
CLUSTERS=[3,4,8]
INIT_SEEDS=[1,2,5,10,20,50]
INIT_TYPE=['k-means++','random']
ITERS=[100,250,500,1000]

#Logging Declarations
logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")

##Most important
mlflow.set_tracking_uri("http://127.0.0.1:5001")

def mlflow_logging_train(data_path):
    """
    model_run: =>model.fit(data)
    """
    try:
        data = pd.read_parquet(data_path)
    except Exception as e:
        logger.exception("Unable to Load file. Error: %s", e)

    for a in INIT_SEEDS:
        for b in ITERS:
            for c in CLUSTERS:
                for d in INIT_TYPE:

                    with mlflow.start_run() as run:
                        run_name = datetime.now().strftime("%Y%m%d-%H%M%S")
                        # Apply KMeans clustering using the optimal k
                        kmeans = KMeans(n_clusters=c, init=d, n_init=a, max_iter=b, random_state=42)
                        kmeans.fit(data)

                        mlflow.log_param("n_clusters", kmeans.n_clusters)
                        print(kmeans.n_clusters)
                        mlflow.log_param("init", kmeans.init)
                        print(kmeans.init)
                        mlflow.log_param("n_init", kmeans.n_init)
                        print(kmeans.n_init)
                        mlflow.log_param("max_iter", kmeans.max_iter)
                        print(kmeans.max_iter)

                        silhouette_=silhouette_score(data, kmeans.labels_, metric='euclidean')
                        mlflow.log_metric("silhouette_score", silhouette_)
                        print(silhouette_)

                        db_score=davies_bouldin_score(data, kmeans.labels_)
                        mlflow.log_metric("davies_bouldin_score", db_score)
                        print(db_score)

                        ch_score=calinski_harabasz_score(data, kmeans.labels_)
                        mlflow.log_metric("calinski_harabasz_score", ch_score)
                        print(ch_score)

                        # Log the model
                        self_predictions=kmeans.predict(data)
                        signature = infer_signature(data,self_predictions)
                        mlflow.sklearn.log_model(kmeans, "model", signature=signature)
                        run_dict={}

                        run_id = run.info.run_id
                        print(f"Run ID: {run_id}")
                        run_dict[f"{run_name}"] = run_id

    p=os.path.join(PAR_DIRECTORY,datetime.now().strftime("%Y%m%d"))
    file=os.path.join(p,datetime.now().strftime("%H%M%S")+".json")
    if not os.path.exists(p):
        os.makedirs(file)

    with open(file,"rb") as f:
        json.dump(run_dict,f,indent=6)

#mlflow_logging_train(__INGESTPATH__)
                    