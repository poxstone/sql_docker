import os
from google.cloud import bigquery

# Especifica el ID de tu proyecto de GCP
project_id = os.environ.get('PROJECT_ID', 'poxs4-datalake-prd')
# Crea un cliente de BigQuery
client = bigquery.Client(project=project_id)

# Lista todos los conjuntos de datos en el proyecto
def get_datasets():
    datasets = list(client.list_datasets())
    datasets_id = []
    
    if datasets:
        print("Conjuntos de datos en el proyecto:")
        for dataset in datasets:
            print(f"ID: {dataset.dataset_id}, Proyecto: {dataset.project}")
            datasets_id.append(dataset.dataset_id)
    else:
        print(f"No se encontraron conjuntos de datos en el proyecto {project_id}.")
    
    return datasets_id


def get_tables_by_dataset(dataset_id):
    # Obtiene una referencia al conjunto de datos
    dataset_ref = client.dataset(dataset_id)
    # Lista todas las tablas en el conjunto de datos
    tables = list(client.list_tables(dataset_ref))
    tables_id = []

    if tables:
        print(f"Tablas en el conjunto de datos {dataset_id}:")
        for table in tables:
            print(f"Nombre de la tabla: {table.table_id}")
            tables_id.append(table.table_id)
    else:
        print(f"No se encontraron tablas en el conjunto de datos {dataset_id}.")


def get_table_schema(dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)
    schema = []
    # Imprime el esquema de la tabla
    print(f"Esquema de la tabla {table_id}:")
    for field in table.schema:
        print(f"Nombre: {field.name}, Tipo: {field.field_type}")
        schema.append({'name':field.name, 'type': field.field_type})
    return schema
    

get_table_schema('dataset_test_persons_01', 'table_user')
