import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from pyspark.sql.functions import col, count

import seaborn as sns

def plot_metrics(metrics, clustering_settings):

    data_per_metric = {}

    # Get right data to plot for each metric
    for metric_name in metrics[0].keys():
        if metric_name == 'settings': # Skip the settings; those are not a type of metric
            continue
        elif metrics[0][metric_name] is None: # See if this metric has None value; should only be the case for perfect centroids
            continue
        else:
            # Get just the scores for this metric
            scores = [metric_dict[metric_name] for metric_dict in metrics]
            data_per_metric[metric_name] = scores

    # Get total num of figures. Make a matplotlib subplot accordingly, that has a maximum of 2 plots per row
    num_figures = len(data_per_metric.keys())
    num_cols = 2
    num_rows = int(np.ceil(num_figures / num_cols))
    
    # Scale figsize accordingly
    figsize = (10, 10 * num_rows / num_cols)
    fig, axes = plt.subplots(num_rows, num_cols, figsize=figsize)

    # Plot each metric
    for idx, metric_name in enumerate(data_per_metric.keys()):
        # Get the right subplot
        row_idx = int(np.floor(idx / num_cols))
        col_idx = idx % num_cols

        # Check if either of the dimensions is 1, and handle choosing right ax indexing accordingly
        if num_rows == 1:
            ax = axes[col_idx]  # 1D array, access with single index
        elif num_cols == 1:
            ax = axes[row_idx]  # 1D array, access with single index
        else:
            ax = axes[row_idx, col_idx]  # 2D array, access with row and column indices

        # Plot the data
        sns.lineplot(x=clustering_settings['k_values'], y=data_per_metric[metric_name], ax=ax)
        ax.set_title(metric_name)
        ax.set_xlabel('k')
        ax.set_ylabel('score')
    return fig

import seaborn as sns
from pyspark.sql.functions import col, count
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import linear_sum_assignment

def get_aligned_df(grouped_df_pd):
    debug1=False
    debug2=False

    # Store the original column order
    old_columns = grouped_df_pd.columns
    if debug2: print(old_columns) 
    
    '''Hungarian algorithm to line up large values on the diagonal'''
    # Perform the Hungarian algorithm to solve the assignment problem
    _, col_ind = linear_sum_assignment(-grouped_df_pd.values)  # We multiply by -1 to find the maximum assignment
    col_names = grouped_df_pd.columns[col_ind]
    reordered_df = grouped_df_pd[col_names]
    '''End Hungarian algorithm'''
    if debug1: print('reordered_df:', reordered_df.head(), '\n')

    # Make a mapping from the old clusters to the new clusters
    old_to_new_cluster_index_map = {old:new for old,new in zip(old_columns, reordered_df.columns)}
    if debug2: print(old_to_new_cluster_index_map)
    
    # Reset the column-names (so the names of predicted clusters) to start from 0
    col_mapping = {col: i for i, col in enumerate(reordered_df.columns)}
    reordered_df.columns = [col_mapping.get(col, len(reordered_df.columns)) for col in reordered_df.columns]

    return reordered_df, old_to_new_cluster_index_map

def plot_confusion_matrix2(clustered_df):
    debug1=False
    debug2=True

    # Get the data from the dataframe into pandas format
    grouped_df = get_confusion_matrix(clustered_df)
    if debug1: print('grouped_df:\n', grouped_df, '\n')

    grouped_df_pd = grouped_df.toPandas()
    if debug1: print('grouped_df:\n', grouped_df_pd.head(), '\n')

    grouped_df_pd = grouped_df_pd.set_index('sr\\predicted_clusters').fillna(0)
    if debug1: print('grouped_df:\n', grouped_df_pd.head(), '\n')

    # Sort DataFrame rows by index of standard routes (so that they go from 0 to ...)
    grouped_df_pd = grouped_df_pd.sort_index(ascending=False)
    
    # Reorder the columns
    reordered_df, old_to_new_cluster_index_map = get_aligned_df(grouped_df_pd)
    if debug2: print(old_to_new_cluster_index_map)


    # Calculate the ideal datapoints per cluster
    total_datapoints = reordered_df.sum().sum()
    ideal_datapoints_per_cluster = total_datapoints / len(reordered_df.index)

    # Normalize each column by its sum or ideal_datapoints_per_cluster, whichever is greater
    column_sums = reordered_df.sum(axis=0)
    normalizing_values = column_sums.where(column_sums > ideal_datapoints_per_cluster, ideal_datapoints_per_cluster)
    normalized_per_column_df = reordered_df.div(normalizing_values, axis=1)

    final_df = normalized_per_column_df
    

    # Now visualize the normalized dataframe. But, use the original values for the annotations.
    fig, ax = plt.subplots(figsize=(10, 10))
    annotation_df = reordered_df.astype(int)
    
    sns.heatmap(final_df,
                annot=annotation_df, # Comment out if you want the normalized value per predicted cluster center
                # annot=True, # Uncomment if you do the above
                fmt='g', cmap='Blues', ax=ax
    )
    ax.set_xlabel('Cluster Center ID')
    ax.set_ylabel('Standard Route ID')
    ax.set_title('Confusion Matrix')

    return fig

    

