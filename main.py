from parameters import *
from queries import *
from google.cloud import bigquery
from google.genai.types import EmbedContentConfig

import pandas as pd
import numpy as np
from sklearn.manifold import TSNE
import matplotlib.pyplot as plt
import seaborn as sns

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
        SELECT driver_ID, stress_report_tags
        FROM `mlops-project-430120.MARTS_DATA.drivers_reason_tags`
        WHERE stress_report_tags IS NOT NULL
        """
    )
    df = client.query(query).result().to_dataframe()

    unique_reasons = df["stress_report_tags"].unique().tolist()
    response = genai_client.models.embed_content(
        model="gemini-embedding-001",
        contents=unique_reasons,
        config=EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
    )
    embeddings = [emb.values for emb in response.embeddings]
    reason_to_embedding = dict(zip(unique_reasons, embeddings))
    df["embedding"] = df["stress_report_tags"].map(reason_to_embedding)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=embedding_schema,
    )

    destination = "mlops-project-430120.MARTS_DATA.driver_reason_embeddings"
    ensure_table_exists(destination, embedding_schema)
    client.load_table_from_dataframe(df, destination, job_config=job_config).result()


def plot_driver_reason_embeddings(table_id):
    """Plot a 2D t-SNE visualization of driver reason embeddings."""

    query = f"""
    SELECT
        driver_ID,
        stress_report_tags,
        embedding
    FROM `{table_id}`
    WHERE embedding IS NOT NULL AND stress_report_tags IS NOT NULL
    """
    df = client.query(query).to_dataframe()
    print(f"Loaded {len(df)} rows from driver_reason_embeddings")

    df["main_tag"] = df["stress_report_tags"].str.split(",").str[0].str.strip()

    X = np.array(df["embedding"].tolist())
    X_embedded = TSNE(n_components=2, perplexity=30, random_state=42).fit_transform(X)

    viz_df = pd.DataFrame(X_embedded, columns=["x", "y"])
    viz_df["main_tag"] = df["main_tag"]

    plt.figure(figsize=(10, 8))
    sns.scatterplot(data=viz_df, x="x", y="y", hue="main_tag", palette="tab10")
    plt.title("Driver Embeddings Colored by First Tag")
    plt.show()


if __name__ == "__main__":

    print("---> Creating drivers_data table to your Bigquery environment.")
    upload_table(csv_path="data/Drivers_Data.csv",
                 schema=drivers_data_schema,
                 table_id=drivers_data_table_id)
    
    print("---> Creating rides_data table to your Bigquery environment.")
    upload_table(csv_path="data/Rides_Data.csv",
                 schema=rides_data_schema,
                 table_id=rides_data_table_id)

    print("---> Creating news_content_usa table to your Bigquery environment.")
    create_table(table_id=news_content_table_id,
                 schema=news_content_usa_schema,
                 append_data=usa_articles,
                 id_column_name="article_name")

    print("---> Creating drivers_metrics view to your Bigquery environment.")
    dm_query = get_drivers_metrics_query(project_id=project_id,
                                         dataset_id=marts_dataset_id,
                                         connection_id=connection_id)
    run_query_and_create_view(view_id=drivers_metrics_table_id,
                              query=dm_query)
    
    print("---> Creating articles_metrics view to your Bigquery environment.")
    am_query = get_drivers_metrics_query(project_id=project_id,
                                         dataset_id=marts_dataset_id,
                                         connection_id=connection_id)
    run_query_and_create_view(view_id=articles_metrics_table_id,
                              query=am_query)

    print("---> Generating driver_reason_embeddings table.")
    generate_driver_reason_embeddings()

    print("---> Visualizing driver_reason_embeddings table.")
    plot_driver_reason_embeddings(table_id=driver_reason_embeddings_table_id)

