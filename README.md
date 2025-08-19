# Gig Workers Data

## **1. Include your GOOGLE_APPLICATION_CREDENTIALS file path to the environment variable**

Where: `parameters.py` (row 4)

```python
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/mariana/.config/gcloud/application_default_credentials.json"
```

If you don't have it yet, run the following command in your terminal:

```bash
gcloud auth application-default login
```

## **2. Include existing project name, dataset names for STAGING DATASET and MARTS DATASET, and `connection_id` from BigQuery**

Where: `parameters.py` (rows 6-9)

```python
project_id = "mlops-project-430120"
staging_dataset_id = "STAGING_DATA"
marts_dataset_id = "MARTS_DATA"
connection_id = 'my-connection'
```

## **3. Run `requirements.txt`**

```bash
pip3 install -r requirements.txt
```

## **4. Run `main.py`**

```bash
python main.py
```

