from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def generate_driver_reason_embeddings(model_cls=None):
    """Generate embeddings for driver stress reasons and store them in BigQuery."""
    if model_cls is None:
        from vertexai.language_models import TextEmbeddingModel
        model_cls = TextEmbeddingModel

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

    try:
        model = model_cls.from_pretrained("textembedding-gecko@003")
    except NotFound:
        model = model_cls.from_pretrained("textembedding-gecko@002")

    unique_reasons = df["stress_reason"].unique().tolist()
    embeddings = model.get_embeddings(unique_reasons)
    reason_to_embedding = {
        reason: emb.values for reason, emb in zip(unique_reasons, embeddings)
    }
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
