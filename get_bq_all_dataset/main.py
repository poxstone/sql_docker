import os
from google.cloud import bigquery

# Especifica el ID de tu proyecto de GCP
project_id = os.environ.get('PROJECT_ID', 'poxs4-datalake-prd')

# Crea un cliente de BigQuery
client = bigquery.Client(project=project_id)

# Lista todos los conjuntos de datos en el proyecto
datasets = list(client.list_datasets())

if datasets:
    print("Conjuntos de datos en el proyecto:")
    for dataset in datasets:
        print(f"ID: {dataset.dataset_id}, Proyecto: {dataset.project}")
else:
    print(f"No se encontraron conjuntos de datos en el proyecto {project_id}.")