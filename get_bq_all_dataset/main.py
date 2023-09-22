import os
import json
import csv
from google.cloud import bigquery

# Especifica el ID de tu proyecto de GCP
project_id = os.environ.get('PROJECT_ID', 'poxs4-datalake-prd')
# Crea un cliente de BigQuery
client = bigquery.Client(project=project_id)
FILE_CSV = "fields.csv"
FILE_JSON = "tables.json"
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
GET_DATA_EXAMPLE = True if os.getenv('GET_DATA_EXAMPLE', 'true').lower() == 'true' else False
LOGS_PRINT = True if os.getenv('LOGS_PRINT', 'false').lower() == 'true' else False 
ERRORS = []


def printing(string, print_logs=True):
    if LOGS_PRINT.lower():
        if print_logs:
            #logging.info(str(string))
            print(f'{str(string)}')
    return ''

# Lista todos los conjuntos de datos en el proyecto
def get_datasets():
    """
    return
    [{'datasetReference': {'datasetId': 'dataset_test_persons_01', 'projectId': 'poxs4-datalake-prd'}, 'id': 'poxs4-datalake-prd:d...persons_01', 'kind': 'bigquery#dataset', 'location': 'US'}]
    """
    datasets = list(client.list_datasets())
    datasets_dic = []
    if datasets:
        printing("Conjuntos de datos en el proyecto:")
        for dataset in datasets:
            datasets_dic.append(dataset.__dict__['_properties'])
            printing(f"ID: {dataset.dataset_id}, Proyecto: {dataset.project}")
    else:
        printing(f"No se encontraron conjuntos de datos en el proyecto {project_id}.")
        datasets = []
    print(f'datasets_cant: {len(datasets)}')
    #return datasets
    return datasets_dic
    

def get_tables_by_dataset(dataset_id):
    """
    response:
    [{'creationTime': '1695311586449', 'id': 'poxs4-datalake-prd:d...able_order', 'kind': 'bigquery#table', 'tableReference': {'datasetId': 'dataset_test_persons_01', 'projectId': 'poxs4-datalake-prd', 'tableId': 'table_order'}, 'type': 'TABLE'}]
    """
    dataset_ref = client.dataset(dataset_id)
    tables = None
    tables_dic = []
    try:
        tables = list(client.list_tables(dataset_ref))
    except Exception as e:
        printing(f'ERROR: {e}')
        ERRORS.append({'dataset': dataset_id, 'error': f'{e}'})
    if tables:
        printing(f"Tablas en el conjunto de datos {dataset_id}:")
        for table in tables:
            printing(f"Nombre de la tabla: {table.table_id}")
            tables_dic.append(table.__dict__['_properties'])
    else:
        printing(f"No se encontraron tablas en el conjunto de datos {dataset_id}.")
        tables = []
    print(f'tables_cant ({dataset_id}): {len(tables)}')
    #return tables
    return tables_dic


def fields_to_dict(fields_main):
    #return [x.__dict__ for x in fields.__dict__['_fields']]
    fields = [x.__dict__ for x in fields_main]
    field_dic = []
    for field in fields:
        field_internal = field['_properties']
        if field['_fields']:
            field_internal['fields'] = fields_to_dict(field['_fields'])
        field_dic.append(field_internal)
    return field_dic


def get_table_schema(dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    schema_dic = []

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
        field_dic = field.__dict__['_properties']
        field_dic['fields'] = []
        if field.__dict__['_fields']:
            #field_dic['fields'] = [x.__dict__ for x in field.__dict__['_fields']]
            field_dic['fields'] = fields_to_dict(field.__dict__['_fields'])
        schema_dic.append(field_dic)
    print(f'working (get_table_schema): {dataset_id}.{table_id}')
    #return table.schema
    return schema_dic


def save_dict_to_file(obj_file):
    with open(FILE_JSON, "w") as json_file:
        json.dump(obj_file, json_file)
    print(f"El diccionario ha sido guardado en '{FILE_JSON}' en formato JSON.")
    return load_file_to_dict(FILE_JSON)


def load_file_to_dict(file_json=FILE_JSON):
    obj_dict = None
    try:
        with open(file_json, "r") as json_file:
            obj_dict = json.load(json_file)
    except Exception as e:
        printing(f'not file found: {e}')
    return obj_dict


def get_all_schemas(project_id=project_id):
    datasets = get_datasets()
    dataset_list = []
    
    for dataset in datasets:
        dataset_id = dataset['datasetReference']['datasetId']
        tables = get_tables_by_dataset(dataset_id)
        dataset['tables'] = []

        for table in tables:
            dataset_id = table['tableReference']['datasetId']
            table_id = table['tableReference']['tableId']
            schema = get_table_schema(dataset_id, table_id)
            table['schema'] = schema
            dataset['tables'].append(table)
    # save to file
    datasets_json = save_dict_to_file(datasets)
    print(f'Errores durante get datasets and tables: {ERRORS}')
    #return datasets  # for debug
    return datasets_json


def do_bq_query(column_name, field_type, table_full_name):
    results_array = []
    if GET_DATA_EXAMPLE == True:
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


def add_subfields(field_main, dataset, table, data, parent_name=''):
    field_name = f'{parent_name}.{field_main["name"]}' if parent_name else field_main["name"]
    if field_main["type"] == 'RECORD':
        for field_sub in field_main["fields"]:
            data = add_subfields(field_sub, dataset, table, data, field_name)
    else:
        printing(f'--loop: {field_name}')
        dataset_id = dataset['datasetReference']['datasetId']
        table_id = table['tableReference']['tableId']
        data_example = do_bq_query(field_name, field_main['type'], f'{project_id}.{dataset_id}.{table_id}')
        data.append(
                        [dataset_id,
                        table_id,
                        field_name,
                        field_main['type'] if 'type' in field_main else '',
                        field_main['mode'] if 'mode' in field_main else '',
                        field_main['default_value_expression'] if 'default_value_expression' in field_main else '',
                        field_main['max_length'] if 'max_length' in field_main else '',
                        field_main['description'] if 'description' in field_main else '',
                        field_main['policy_tags'] if 'policy_tags' in field_main else '',
                        data_example]
                    )
    return data


def do_fields_csv():
    dataset_list = load_file_to_dict(FILE_JSON)
    if not dataset_list:
        dataset_list = get_all_schemas()
    data = [
        SCHEMA_CSV,
    ]
    for dataset in dataset_list:
        print(f"working (do csv): {dataset['id']}")
        for table in dataset['tables']:
            for field_main in table['schema']:
                #printing(f'{dataset['dataset_id']},{table['table_id']},{field.name},{field.field_type}')
                printing(f'main: {field_main["name"]}')
                data = add_subfields(field_main, dataset, table, data)
            if GET_DATA_EXAMPLE:
                print(f"working (get sql example): {table['id']}")

    with open(FILE_CSV, mode="w", newline="") as archivo_csv:
        escritor = csv.writer(archivo_csv)
        for fila in data:
            escritor.writerow(fila)

    print(f"El archivo CSV '{FILE_CSV}' ha sido creado con éxito.")


#get_datasets()
#get_tables_by_dataset('dataset_test_persons_01')
#get_table_schema('dataset_test_persons_01', 'table_order')
#dataset_list = get_all_schemas()
#printing(dataset_list)
do_fields_csv()