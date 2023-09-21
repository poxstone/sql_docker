import os
import json
from google.cloud import bigquery

# Especifica el ID de tu proyecto de GCP
project_id = os.environ.get('PROJECT_ID', 'poxs4-datalake-prd')
# Crea un cliente de BigQuery
client = bigquery.Client(project=project_id)

# Lista todos los conjuntos de datos en el proyecto
def get_datasets():
    datasets = list(client.list_datasets())
    if datasets:
        print("Conjuntos de datos en el proyecto:")
        for dataset in datasets:
            print(f"ID: {dataset.dataset_id}, Proyecto: {dataset.project}")

    else:
        print(f"No se encontraron conjuntos de datos en el proyecto {project_id}.")
        datasets = []
    return datasets
    

def get_tables_by_dataset(dataset_id):
    # Obtiene una referencia al conjunto de datos
    dataset_ref = client.dataset(dataset_id)
    # Lista todas las tablas en el conjunto de datos
    tables = list(client.list_tables(dataset_ref))

    if tables:
        print(f"Tablas en el conjunto de datos {dataset_id}:")
        for table in tables:
            print(f"Nombre de la tabla: {table.table_id}")
    else:
        print(f"No se encontraron tablas en el conjunto de datos {dataset_id}.")
        tables = []
    return tables


def get_table_schema(dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)
    schema = []
    # Imprime el esquema de la tabla
    print(f"Esquema de la tabla {table_id}:")
    for field in table.schema:
        print(f"Nombre: {field.name}, Tipo: {field.field_type}")
    return table.schema


def get_all_schemas(project_id=project_id):
    datasets = get_datasets()
    dataset_list = []
    
    for dataset in datasets:
        tables = get_tables_by_dataset(dataset.dataset_id)
        dataset_dic = {'dataset_id': dataset.dataset_id, 'tables': []}

        for table in tables:
            schema = get_table_schema(table.dataset_id, table.table_id)
            table_dic = {'table_id': table.table_id, 'schema': schema}
            dataset_dic['tables'].append(table_dic)
        
        dataset_list.append(dataset_dic)
    
    return dataset_list
            


#get_datasets()
#get_tables_by_dataset('dataset_test_persons_01')
#get_table_schema('dataset_test_persons_01', 'table_order')
dataset_list = get_all_schemas()
print(dataset_list)
