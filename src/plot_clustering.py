import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from pyspark.sql.functions import col, substring,  countDistinct, count


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

def get_confusion_matrix(clustered_df):
    # Generate a table which has the standard routes as rows and the cluster centres as columns. It shows for each found cluster center,
    #  which route it belongs to; this is because the datapoints in each cluster are grouped per standard-route-id.
    grouped_df = clustered_df.withColumn("sr\predicted_clusters", col("id-sr").substr(-1, 1)) \
    .groupBy("sr\\predicted_clusters") \
    .pivot("prediction") \
    .agg(count("*").alias("count"))

    return grouped_df


import seaborn as sns

def plot_confusion_matrix(grouped_df):
    """
    Plots the confusion matrix.

    Args:
        grouped_df: A DataFrame containing the confusion matrix data.

    Returns:
        A matplotlib figure representing the confusion matrix.
    """

    # The id-number of the found cluster centers does not necessarily match the id-number of the standard routes.
    # Therefore, we need to map the found cluster center id's to the standard route id's, by seeing which cluster center
    # it corresponds with best. This is done by taking the max index (in terms of the row) for each column. 
    # This is the standard route id that corresponds with the cluster center. However, a problem is there can be more or fewer cluster centers
    # than standard routes. Therefore, we need to check if there are duplicates in the resulting list of max-indices. The highest of the two, should be assigned 
    # to that cluster center. The other one should be assigned to the next-best cluster-center, but only if it is a better match than the current one. Then, the current one
    # needs to be checked, and so on. Since there may be more centers than routes, it is okay that some get an id higher than the number of routes.


    # Get the data from the dataframe into pandas format
    grouped_df_pd = grouped_df.toPandas()
    grouped_df_pd = grouped_df_pd.set_index('sr\\predicted_clusters').fillna(0)

    # 1. Get max indices
    max_indices = grouped_df_pd.idxmax()

    # 2. Handle duplicate max indices
    max_values = grouped_df_pd.max()
    for idx, val in max_indices.duplicated(keep=False).items():
        if val:
            duplicates = max_indices[max_indices == max_indices[idx]]
            for dup in duplicates.index:
                # 3. Reassign to next best
                if max_values[dup] < max_values[idx]:
                    max_indices[dup] = grouped_df_pd[dup].nlargest(2).index[1]

    # 4. Handling more cluster centers than routes
    max_indices_list = max_indices.tolist()
    if len(max_indices_list) > len(set(max_indices_list)):
        unique_vals = list(set(max_indices_list))
        duplicates = list(set([x for x in max_indices_list if max_indices_list.count(x) > 1]))
        for dup in duplicates:
            indices = [i for i, x in enumerate(max_indices_list) if x == dup]
            for ind in indices[1:]:
                max_indices_list[ind] = max(unique_vals) + 1
                unique_vals.append(max_indices_list[ind])

    # Reorder dataframe columns based on max_indices_list
    grouped_df_pd = grouped_df_pd[max_indices_list]
    

    # Create the heatmap
    fig, ax = plt.subplots(figsize=(10, 10))
    sns.heatmap(grouped_df_pd, annot=True, fmt='g', cmap='Blues', ax=ax)
    ax.set_xlabel('Cluster Center ID')
    ax.set_ylabel('Standard Route ID')
    ax.set_title('Confusion Matrix')

    return fig

    

