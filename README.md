# Gig Workers Data

This is a Data Generator Tool to analyze driver stress levels based on drivers' operational metrics. We are using simulated data from Gig Workers (drivers) to build this experiment. Gig work is highly flexible, but platform policies, ML-driven management, low payments, blocked accounts, and long idle times can strongly affect drivers’ mental well-being.

Here is the public Kaggle notebook link: https://www.kaggle.com/code/mariihespana/gig-workers-stress-levels

To run the analysis on Kaggle, please see the instructions below:

## 1. Enable the Google Cloud SDK

1. In the right sidebar of your Kaggle notebook, go to **"Add-ons" → "Google Cloud SDK"**.
2. Click **"Enable"** to give the notebook access to Google Cloud services such as BigQuery.

## **2. Include existing project name, dataset names for STAGING DATASET and MARTS DATASET, and `connection_id` from BigQuery**

Where: `parameters.py` (rows 6-9)

```python
project_id = "mlops-project-430120"
staging_dataset_id = "STAGING_DATA"
marts_dataset_id = "MARTS_DATA"
connection_id = 'my-connection'
```

## **3. Replace `genai_api_key` with your Google Gemini API key**

Where: `parameters.py` (row 20)

Get an API key from [Google AI Studio](https://aistudio.google.com/app/apikey) and assign it to `genai_api_key` in `parameters.py`.

## **4. Run `requirements.txt`**

```bash
pip3 install -r requirements.txt
```
or 
```bash
pip install -r requirements.txt
```

## **5. Run `main.py`**

```bash
python main.py
```
or
```bash
python3 main.py
```

