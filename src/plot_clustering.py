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

def get_confusion_matrix(clustered_df):
    # Generate a table which has the standard routes as rows and the cluster centres as columns. It shows for each found cluster center,
    #  which route it belongs to; this is because the datapoints in each cluster are grouped per standard-route-id.
    grouped_df = clustered_df.withColumn("sr\predicted_clusters", col("id-sr").substr(-1, 1)) \
    .groupBy("sr\\predicted_clusters") \
    .pivot("prediction") \
    .agg(count("*").alias("count"))

    return grouped_df


def plot_confusion_matrix(clustered_df):
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

    debug1=False
    debug2=True

    # Get the data from the dataframe into pandas format
    grouped_df = get_confusion_matrix(clustered_df)
    grouped_df_pd = grouped_df.toPandas()
    grouped_df_pd = grouped_df_pd.set_index('sr\\predicted_clusters').fillna(0)

    # Sort DataFrame by index of standard routes
    grouped_df_pd = grouped_df_pd.sort_index()

    # 1. Get max indices
    max_indices = grouped_df_pd.idxmax()

    if debug1: print(grouped_df_pd, '\n')
    if debug1: print('max_indices:',max_indices)

    # 2. Handle duplicate max indices

    # Normalize the data to reflect the "strength" of the association between the clusters and the routes
    grouped_df_pd_normalized = grouped_df_pd.divide(grouped_df_pd.sum(axis=0), axis=1)

    # 2. Handle duplicate max indices
    max_values = grouped_df_pd_normalized.max()
    assigned_clusters = []
    unassigned_routes = []

    for idx, val in max_indices.duplicated(keep=False).items():
        if val:
            duplicates = max_indices[max_indices == max_indices[idx]]
            for dup in duplicates.index:
                # 3. Reassign to next best
                if max_values[dup] < max_values[idx] or idx in assigned_clusters:
                    new_assignment = grouped_df_pd_normalized[dup].nlargest(2).index[1]
                    if new_assignment in assigned_clusters:
                        unassigned_routes.append(idx)
                    else:
                        max_indices[dup] = new_assignment
                        assigned_clusters.append(new_assignment)
                else:
                    assigned_clusters.append(idx)
    

    # 4. Handling more cluster centers than routes
    max_indices_list = max_indices.tolist()

    if debug1: print('max_indices_list',max_indices_list)

    if len(max_indices_list) > len(set(max_indices_list)):
        unique_vals = list(set(max_indices_list))
        duplicates = list(set([x for x in max_indices_list if max_indices_list.count(x) > 1]))
        for dup in duplicates:
            indices = [i for i, x in enumerate(max_indices_list) if x == dup]
            for ind in indices[1:]:
                max_indices_list[ind] = max(unique_vals) + 1
                unique_vals.append(max_indices_list[ind])
        
        if debug1: print('unique_vals', unique_vals)
        if debug1: print('duplicates', duplicates)
    else:
        if debug1: print('not len(max_indices_list) > len(set(max_indices_list))')

    # 5. Handling fewer cluster centers than routes

    if debug1: print(f"unassigned routes: {unassigned_routes}")

    for unassigned in unassigned_routes:
        max_indices_list[unassigned] = max(unique_vals) + 1
        unique_vals.append(max_indices_list[unassigned])

    # Create a mapping from column names to their new indices.
    col_mapping = {col: i for i, col in enumerate(max_indices_list)}

    # Reorder the columns by the mapped indices.

    # The key=lambda x: [col_mapping.get(col, len(max_indices_list)) for col in x]
    # part will use the index in max_indices_list if the column is present there,
    # or the length of max_indices_list otherwise. This will place any extra columns
    # that are not in max_indices_list at the end.
    grouped_df_pd = grouped_df_pd.sort_index(axis=1, key=lambda x: [col_mapping.get(col, len(max_indices_list)) for col in x])
    # if debug1: print(grouped_df_pd)   

    # Normalization and deviation score calculation
    normalized_grouped_df_pd = grouped_df_pd.divide(grouped_df_pd.sum(axis=0), axis=1)

    # Reset the indices of the predicted clusters (the columns), making them start from 0. However, save the mapping from the old indices to the new indices.
    col_mapping = {col: i for i, col in enumerate(normalized_grouped_df_pd.columns)}
    normalized_grouped_df_pd.columns = [col_mapping.get(col, len(normalized_grouped_df_pd.columns)) for col in normalized_grouped_df_pd.columns]

    ''' NOTE: here we  try to set the highest elements on the diagonal, using ...'''
    # Now let's iteratively find the max element, and reorder columns so that this element goes to the diagonal
    # for _ in range(len(normalized_grouped_df_pd.columns)):
    #     # Get the coordinates of the max element
    #     max_row_label = normalized_grouped_df_pd.max(axis=1).idxmax()
    #     max_col_label = normalized_grouped_df_pd.loc[max_row_label].idxmax()

    #     # Convert labels to integer indices
    #     max_row = normalized_grouped_df_pd.index.to_list().index(max_row_label)
    #     max_col = normalized_grouped_df_pd.columns.to_list().index(max_col_label)

    #     # Place the column of the max element to the position of its row index
    #     cols = list(normalized_grouped_df_pd.columns)
    #     cols.remove(max_col_label)
    #     cols.insert(max_row, max_col_label)
    #     normalized_grouped_df_pd = normalized_grouped_df_pd[cols]

    #     # Remove this row and column from the dataframe
    #     normalized_grouped_df_pd = normalized_grouped_df_pd.drop(max_row_label)
    #     normalized_grouped_df_pd = normalized_grouped_df_pd.drop(max_col_label, axis=1)

    ''' END: of setting highest values on diagonal'''
    
    final_df = normalized_grouped_df_pd

    if debug1: print(final_df)

    # Now we can use this `deviation_score` dataframe to visualize it.
    fig, ax = plt.subplots(figsize=(10, 10))
    sns.heatmap(final_df, annot=True, fmt='g', cmap='Blues', ax=ax)
    ax.set_xlabel('Cluster Center ID')
    ax.set_ylabel('Standard Route ID')
    ax.set_title('Confusion Matrix Deviation Score')

    return fig



    

