from google.cloud import bigquery
from google import genai
from google.genai.types import EmbedContentConfig


def generate_driver_reason_embeddings():
    """Generate embeddings for driver stress reasons and store them in BigQuery."""
    bq_client = bigquery.Client(project="mlops-project-430120")
    query = (
        """
        SELECT driver_ID, stress_reason
        FROM `mlops-project-430120.MARTS_DATA.tbl_drivers_metrics`
        WHERE stress_reason IS NOT NULL
        """
    )
    df = bq_client.query(query).result().to_dataframe()
    if df.empty:
        return

    client = genai.Client(
        vertexai=True, project="mlops-project-430120", location="us-central1"
    )
    unique_reasons = df["stress_reason"].unique().tolist()
    response = client.models.embed_content(
        model="gemini-embedding-001",
        contents=unique_reasons,
        config=EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
    )
    embeddings = [emb.values for emb in response.embeddings]
    reason_to_embedding = dict(zip(unique_reasons, embeddings))
    df["embedding"] = df["stress_reason"].map(reason_to_embedding)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("driver_ID", "STRING"),
            bigquery.SchemaField("stress_reason", "STRING"),
            bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
        ],
    )
    destination = "mlops-project-430120.STAGING_DATA.driver_reason_embeddings"
    bq_client.load_table_from_dataframe(df, destination, job_config=job_config).result()
