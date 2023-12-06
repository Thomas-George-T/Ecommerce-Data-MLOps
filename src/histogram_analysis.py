"""
Module for Analyzing Customer Clusters
using Radar Chart
"""
import os
import pandas as pd
#import plotly.express as px
#import plotly.graph_objects as go
#from dash import Dash, dcc, html
import matplotlib.pyplot as plt

PAR_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#Global variables
__INGESTPATH__ = os.path.join(PAR_DIR,"data","processed","df_cleaned.parquet")
__SAVEPATH__= os.path.join(PAR_DIR,"data/plots")
colors = ['#e8000b', '#023eff', '#1ac938']

def rfm_histograms(in_path=__INGESTPATH__):
    """
    Docstring
    """
    #Placeholder for data
    df_customer = None

    #File Loading
    try:
        df_customer = pd.read_parquet(in_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found at {in_path}.") from None

    #Plot histograms for each feature segmented by the clusters
    features = df_customer.columns[1:-1]
    clusters = df_customer['cluster'].unique()
    clusters.sort()

    # Setting up the subplots
    n_rows = len(features)
    n_cols = len(clusters)

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(48, 6*n_rows))
    fig.tight_layout()
    plt.rcParams.update({'font.size': 12})

    # Plotting histograms
    for i, feature in enumerate(features):
        for j, cluster in enumerate(clusters):
            data = df_customer[df_customer['cluster'] == cluster][feature]
            axes[i, j].hist(data, bins=20, color=colors[j], edgecolor='w', alpha=0.7)
            axes[i, j].set_title(f'Cluster {cluster} - {feature}', fontsize=10)
            axes[i, j].set_xlabel('')
            axes[i, j].set_ylabel('')

    # Adjusting layout to prevent overlapping
    fig.show()

    if not os.path.exists(__SAVEPATH__):
        os.makedirs(__SAVEPATH__)
    fig.savefig(os.path.join(__SAVEPATH__,"histogram_analysis.png"),bbox_inches='tight')

#rfm_histograms()
