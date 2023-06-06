import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.decomposition import PCA

def plot_routes(data:pd.DataFrame):
    le = LabelEncoder()
    data['from'] = le.fit_transform(data['from'])
    data['to'] = le.fit_transform(data['to'])

    # Normalize data
    scaler = StandardScaler()
    scaled_df = scaler.fit_transform(data)

    # Apply PCA
    pca = PCA(n_components=2)  # 2 for visualization in 2D space
    principal_components = pca.fit_transform(scaled_df)

    # Convert the principal components into a DataFrame
    principal_df = pd.DataFrame(data=principal_components, columns=['PC1', 'PC2'])

    print(principal_df)

    # Transform the data
    # df = pca.fit_transform(data)
    # print(df.shape)

def plot_results(results):
    pass
