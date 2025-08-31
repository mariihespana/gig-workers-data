# Gig Workers Data

### **1. Enable BigQuery Access

1. In the right sidebar of your Kaggle notebook, go to **"Add-ons" → "Google Cloud"**.
2. Click **"Enable"** to grant BigQuery access to the notebook.

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
or 
```bash
pip install -r requirements.txt
```

## **4. Run `main.py`**

```bash
python main.py
```
or
```bash
python3 main.py
```

