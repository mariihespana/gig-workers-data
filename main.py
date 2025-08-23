from parameters import *
from queries import *
from google.cloud import bigquery
from google.genai.types import EmbedContentConfig

def ensure_table_exists(table_id, schema):
    """Create the given BigQuery table if it does not already exist."""
    try:
        client.get_table(table_id)
        print(f"Table already exists: {table_id}")
    except Exception:
        print(f"Table not found. Creating table: {table_id}")
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"Table created: {table_id}")


def create_table(table_id, schema, append_data, id_column_name):
    """Create a BigQuery table if missing and append data rows."""
    ensure_table_exists(table_id, schema)

    rows_added = 0
    for item in append_data:
        errors = client.insert_rows_json(table_id, [item])
        if errors:
            print(f"Failed to add row for '{item[id_column_name]}': {errors}")
        else:
            rows_added += 1
            print(f"Added row {rows_added}: {item[id_column_name]}")

    print(f"Total rows added: {rows_added}")


def upload_table(csv_path, schema, table_id):
    """
    Load a CSV file into a BigQuery table, replacing existing data.

    Parameters:
    - csv_path: str. Local path to the CSV file.
    - schema: list[bigquery.SchemaField]. Schema for the destination table.
    - table_id: str. Full table path where the data will be loaded.
    """

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # skip header
        autodetect=True,  # set to True if you want BigQuery to infer schema
        schema=schema,
    )

    # === Load to BigQuery ===
    with open(csv_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Wait for completion
    print(f"Loaded {job.output_rows} rows to {table_id}")

def run_query_and_create_view(view_id, query):
    """
    Create or replace a BigQuery view with the given query.

    Parameters:
    - view_id: str. Full view path in the format "project_id.dataset_id.view_name"
    - query: str. SQL query to be used in the view definition
    """
    try:
        view = bigquery.Table(view_id)
        view.view_query = query
        view = client.create_table(view, exists_ok=True)  # replaces if exists
        print(f"View created or updated: {view_id}")
    except Exception as e:
        print(f"Failed to create view: {e}")

def generate_driver_reason_embeddings():
    """Generate embeddings for driver stress reasons and store them in BigQuery."""
    query = (
        """
        SELECT Driver_ID, stress_reason
        FROM `mlops-project-430120.MARTS_DATA.tbl_drivers_metrics`
        WHERE stress_reason IS NOT NULL
        """
    )
    df = client.query(query).result().to_dataframe()

    unique_reasons = df["stress_reason"].unique().tolist()
    response = genai_client.models.embed_content(
        model="gemini-embedding-001",
        contents=unique_reasons,
        config=EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
    )
    embeddings = [emb.values for emb in response.embeddings]
    reason_to_embedding = dict(zip(unique_reasons, embeddings))
    df["embedding"] = df["stress_reason"].map(reason_to_embedding)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=embedding_schema,
    )

    destination = "mlops-project-430120.MARTS_DATA.driver_reason_embeddings"
    ensure_table_exists(destination, embedding_schema)
    client.load_table_from_dataframe(df, destination, job_config=job_config).result()


if __name__ == "__main__":

    # print("---> Creating drivers_data table to your Bigquery environment.")
    # upload_table(csv_path="data/Drivers_Data.csv",
    #              schema=drivers_data_schema,
    #              table_id=drivers_data_table_id)
    
    # print("---> Creating rides_data table to your Bigquery environment.")
    # upload_table(csv_path="data/Rides_Data.csv",
    #              schema=rides_data_schema,
    #              table_id=rides_data_table_id)

    # print("---> Creating news_content_usa table to your Bigquery environment.")
    # create_table(table_id=news_content_table_id,
    #              schema=news_content_usa_schema,
    #              append_data=usa_articles,
    #              id_column_name="article_name")

    # print("---> Creating drivers_metrics view to your Bigquery environment.")
    # dm_query = get_drivers_metrics_query(project_id=project_id,
    #                                      dataset_id=marts_dataset_id,
    #                                      connection_id=connection_id)
    # run_query_and_create_view(view_id=drivers_metrics_table_id,
    #                           query=dm_query)
    
    # print("---> Creating articles_metrics view to your Bigquery environment.")
    # am_query = get_drivers_metrics_query(project_id=project_id,
    #                                      dataset_id=marts_dataset_id,
    #                                      connection_id=connection_id)
    # run_query_and_create_view(view_id=articles_metrics_table_id,
    #                           query=am_query)

    # print("---> Generating driver_reason_embeddings table.")
    # generate_driver_reason_embeddings()

    from google.cloud import bigquery
    import pandas as pd
    import networkx as nx
    import matplotlib.pyplot as plt
    from pyvis.network import Network

    # ---- CONFIG ----
    PROJECT_ID = "mlops-project-430120"
    TABLE_ID = "mlops-project-430120.MARTS_DATA.tbl_drivers_similarity"

    query = """
    WITH stress_pairs AS (
    SELECT
        ROUND(s.distance, 2) AS distance_bin,
        ABS(m.stress_score - n.stress_score) AS stress_diff
    FROM `mlops-project-430120.MARTS_DATA.tbl_drivers_similarity` s
    LEFT JOIN `mlops-project-430120.MARTS_DATA.tbl_drivers_metrics` m
        ON s.query_driver_id = m.driver_ID
    LEFT JOIN `mlops-project-430120.MARTS_DATA.tbl_drivers_metrics` n
        ON s.base_driver_id = n.driver_ID
    WHERE s.query_driver_id <> s.base_driver_id
    )
    SELECT
    distance_bin,
    COUNT(*) AS num_pairs,
    SUM(CASE WHEN stress_diff <= 0.15 THEN 1 ELSE 0 END) AS similar_pairs,
    SUM(CASE WHEN stress_diff > 0.15 THEN 1 ELSE 0 END) AS different_pairs,
    ROUND(SUM(CASE WHEN stress_diff <= 0.15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_similar
    FROM stress_pairs
    GROUP BY distance_bin
    ORDER BY distance_bin;
    """

    import matplotlib.pyplot as plt
    import numpy as np
    df_plot = client.query(query).to_dataframe()
    print(df_plot.head())

    # Data prep
    df_plot['distance_bin'] = df_plot['distance_bin'].astype(str)
    df_plot['pct_different'] = 1 - df_plot['pct_similar']

    # Create positions for bars
    x = np.arange(len(df_plot['distance_bin']))

    plt.figure(figsize=(8, 5))

    # Stacked bars
    bars_sim = plt.bar(x, df_plot['pct_similar'], label='Similar', color='green')
    bars_diff = plt.bar(x, df_plot['pct_different'], 
                        bottom=df_plot['pct_similar'], label='Different', color='red')

    # Add total number of pairs on top of bars
    for i, total in enumerate(df_plot['num_pairs']):
        plt.text(x[i], 1.02, str(total), ha='center', fontsize=9, fontweight='bold')

    # Axis settings
    plt.xticks(x, df_plot['distance_bin'])
    plt.xlabel('Distance Bin')
    plt.ylabel('% of Pairs')
    plt.title('Similarity vs Distance')
    plt.ylim(0, 1.15)
    plt.legend()
    plt.tight_layout()
    plt.show()




    # df = client.query(query).to_dataframe()
    # df = df[df["stress_score"] >= 0.4]

    # import matplotlib.pyplot as plt
    # import seaborn as sns

    # # # Plot histogram + KDE for stress score
    # # plt.figure(figsize=(10, 6))
    # # sns.histplot(df['stress_score'].dropna(), bins=20, kde=True, color="skyblue")
    # # plt.title("Stress Score Distribution")
    # # plt.xlabel("Stress Score")
    # # plt.ylabel("Number of Drivers")
    # # plt.grid(axis='y', linestyle='--', alpha=0.6)
    # # plt.show()

    # # # Check descriptive statistics
    # # print(df['stress_score'].describe())

    # print(f"Loaded {len(df)} driver similarity pairs.")
    # # Count drivers per stress_score group
    # bins = [0, 0.2, 0.4, 1.0]
    # labels = ["Low (≤0.2)", "Medium (0.2-0.4)", "High (>0.4)"]

    # df["stress_group"] = pd.cut(df["stress_score"], bins=bins, labels=labels, include_lowest=True)
    # print(df["stress_group"].value_counts())


    # # ---- STEP 2. Build Graph ----
    # G = nx.Graph()

    # # Add edges with similarity as weight
    # for _, row in df.iterrows():
    #     G.add_edge(
    #         row["query_driver_id"],
    #         row["base_driver_id"],
    #         weight=row["distance"]
    #     )

    # print(f"Total nodes: {len(G.nodes())}")
    # print(f"Total edges: {len(G.edges())}")

    # # ---- STEP 3. Static Visualization ----
    # plt.figure(figsize=(12, 8))
    # pos = nx.spring_layout(G, k=0.5)

    # # Color map for stress groups
    # color_map = {
    #     "Low (≤0.2)": "#4CAF50",    # green
    #     "Medium (0.2-0.4)": "#FFC107", # yellow
    #     "High (>0.4)": "#F44336"    # red
    # }

    # # Assign colors to nodes
    # node_colors = []
    # for node in G.nodes():
    #     score = df.loc[df["driver_ID"] == node, "stress_score"].values
    #     if len(score) > 0:
    #         group = pd.cut([score[0]], bins=bins, labels=labels, include_lowest=True)[0]
    #         node_colors.append(color_map[group])
    #     else:
    #         node_colors.append("#B0BEC5")  # gray for missing scores
    
    #     import networkx.algorithms.community as nx_comm

    # communities = nx_comm.louvain_communities(G, weight="weight")
    # print(f"Found {len(communities)} clusters")

    # # Assign cluster color per node
    # cluster_map = {}
    # for i, comm in enumerate(communities):
    #     for node in comm:
    #         cluster_map[node] = i

    # node_colors_by_cluster = [cluster_map[n] for n in G.nodes()]

    # nx.draw_networkx_nodes(
    #     G, pos,
    #     # node_size=[G.nodes[n].get("stress_score", 0.1) * 300 for n in G.nodes()],
    #     node_size=300,
    #     node_color=node_colors,
    #     cmap=plt.cm.viridis
    # )    

    # # edge_weights = [1 / (data["weight"] + 0.01) for _, _, data in G.edges(data=True)]
    # # nx.draw_networkx_edges(G, pos, alpha=0.3, width=edge_weights)

    # nx.draw_networkx_edges(G, pos, alpha=0.3)
    # nx.draw_networkx_labels(G, pos, font_size=6)

    # plt.title("Driver Stress Similarity Network")
    # plt.axis("off")
    # plt.show()

    # # ---- STEP 4. Interactive Visualization ----
    # net = Network(height="800px", width="100%", notebook=False)
    # for node in G.nodes():
    #     net.add_node(node, title=f"Driver {node}")

    # for u, v, data in G.edges(data=True):
    #     net.add_edge(u, v, value=data["weight"])

    # net.show("driver_similarity_network.html")
    # print("Interactive network saved: driver_similarity_network.html")


