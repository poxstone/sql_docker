import os
import json
import csv
from google.cloud import bigquery

# Especifica el ID de tu proyecto de GCP
project_id = os.environ.get('PROJECT_ID', 'poxs4-datalake-prd')
# Crea un cliente de BigQuery
client = bigquery.Client(project=project_id)
FILE_CSV = "fields.csv"
SCHEMA_CSV = [
    'dataset',
    'table',
    'field_name',
    'field_type',
    'field_mode',
    'default_value_expresion',
    'max_length',
    'field_description',
    'policy_tags',
    'example_value']
GET_DATA_EXAMPLE = True
LOGS_PRINT = os.getenv('LOGS_PRINT', 'false')
ERRORS = []


def printing(string, print_logs='true'):
    if LOGS_PRINT.lower() == 'true':
        if print_logs == 'true':
            #logging.info(str(string))
            print(f'{str(string)}')
    return ''

# Lista todos los conjuntos de datos en el proyecto
def get_datasets():
    datasets = list(client.list_datasets())
    if datasets:
        printing("Conjuntos de datos en el proyecto:")
        for dataset in datasets:
            printing(f"ID: {dataset.dataset_id}, Proyecto: {dataset.project}")
    else:
        printing(f"No se encontraron conjuntos de datos en el proyecto {project_id}.")
        datasets = []
    return datasets
    

def get_tables_by_dataset(dataset_id):
    # Obtiene una referencia al conjunto de datos
    dataset_ref = client.dataset(dataset_id)
    # Lista todas las tablas en el conjunto de datos
    tables = None
    try:
        tables = list(client.list_tables(dataset_ref))
    except Exception as e:
        printing(f'ERROR: {e}')
        ERRORS.append({'dataset': dataset_id, 'error': f'{e}'})
    if tables:
        printing(f"Tablas en el conjunto de datos {dataset_id}:")
        for table in tables:
            printing(f"Nombre de la tabla: {table.table_id}")
    else:
        printing(f"No se encontraron tablas en el conjunto de datos {dataset_id}.")
        tables = []
    return tables


def get_table_schema(dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        table = client.get_table(table_ref)
    except Exception as e:
        printing(f'ERROR: {e}')
        ERRORS.append({'table': table_id, 'error': f'{e}'})
        return []

    # Imprime el esquema de la tabla
    printing(f"Esquema de la tabla {table_id}:")
    for field in table.schema:
        printing(f"Nombre: {field.name}, Tipo: {field.field_type}")
    print(f'working: {dataset_id}.{table_id}')
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


def do_bq_query(column_name, field_type, table_full_name):
    results_array = []
    if GET_DATA_EXAMPLE:
        modificator = 'DISTINCT' if not field_type in ['GEOGRAPHY', 'TIMESTAMP', 'DATE', 'TIME', 'DATETIME', 'BYTES', 'STRUCT'] else ''
        sql_query = f"""SELECT {modificator} {column_name} FROM `{table_full_name}` LIMIT 10"""
        query_job = client.query(sql_query)
        results = query_job.result()
        results = list(query_job.result())
        # Convierte los resultados en un array de diccionarios
        for row in results:
            printing(row)
        #results_array = [row.get(column_name) for row in results]
        results_array = ' | '.join(map(str, [row.get(column_name) for row in results]))
    return results_array


def add_subfields(field_main, dataset, table, data, parent_name=None):
    field_name = f'{parent_name}.{field_main.name}' if parent_name else field_main.name
    if field_main.field_type == 'RECORD':
        for field_sub in field_main.fields:
            data = add_subfields(field_sub, dataset, table, data, field_name)
    else:
        printing(f'--loop: {field_name}')
        dataset_id = dataset['dataset_id']
        table_id = table['table_id']
        data_example = do_bq_query(field_name, field_main.field_type, f'{project_id}.{dataset_id}.{table_id}')
        data.append(
                        [dataset_id,
                        table_id,
                        field_name,
                        field_main.field_type,
                        field_main.mode,
                        field_main.default_value_expression,
                        field_main.max_length,
                        field_main.description,
                        field_main.policy_tags,
                        data_example]
                    )
    return data


def do_fields_csv():
    dataset_list = get_all_schemas()
    data = [
        SCHEMA_CSV,
        #['dataset_test_persons_01','table_order','orderId','INTEGER',"0,2,3"]
    ]
    for dataset in dataset_list:
        for table in dataset['tables']:
            for field_main in table['schema']:
                #printing(f'{dataset['dataset_id']},{table['table_id']},{field.name},{field.field_type}')
                printing(f'main: {field_main.name}')
                data = add_subfields(field_main, dataset, table, data)

    with open(FILE_CSV, mode="w", newline="") as archivo_csv:
        escritor = csv.writer(archivo_csv)
        for fila in data:
            escritor.writerow(fila)

    printing(f"El archivo CSV '{FILE_CSV}' ha sido creado con éxito.")


#get_datasets()
#get_tables_by_dataset('dataset_test_persons_01')
#get_table_schema('dataset_test_persons_01', 'table_order')
#dataset_list = get_all_schemas()
#printing(dataset_list)
do_fields_csv()