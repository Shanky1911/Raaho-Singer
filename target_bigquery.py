import logging
import singer
import json
import datetime
import sys
from google.cloud import bigquery
import datetime
import pytz
from google.cloud.exceptions import NotFound

REQUIRED_CONFIG_KEYS = ['keyfile_path', 'project_id', 'dataset_id']


def convert_pgadmin_to_bigquery(data_type,udt_name):
    data_type = data_type.lower()
    if data_type.startswith('int') or data_type.startswith('serial') or data_type.startswith('bigserial') or data_type.startswith('bigint') or data_type.startswith('smallint'):
        return 'INT64'
    elif data_type.startswith('numeric') or data_type.startswith('decimal'):
        return 'NUMERIC'
    elif data_type.startswith('float') or data_type.startswith('double') or data_type.startswith('real'):
        return 'FLOAT64'
    elif data_type.startswith('boolean'):
        return 'BOOL'
    elif data_type.startswith('json'):
        return 'JSONB'
    elif data_type.startswith('character') or data_type.startswith('char') or data_type.startswith('bpchar') or data_type.startswith('text') or data_type.startswith('citext'):
        return 'STRING'
    elif data_type.startswith('date'):
        return 'DATE'
    elif data_type.startswith('timestamp') or data_type.startswith('timestamptz'):
        return 'TIMESTAMP'
    elif data_type.startswith('time') or data_type.startswith('timetz'):
        return 'TIME'
    elif data_type.startswith('interval'):
        return 'STRING'
    elif data_type.startswith('bytea') or data_type.startswith('blob'):
        return 'BYTES'
    elif data_type == 'array':
        # Check for specific array types
        if 'int' in udt_name:
            return 'INT64'
        elif 'timestamp' in data_type:
            return 'TIMESTAMP'
        elif 'varchar' in data_type:
            return 'STRING'
        elif 'numeric' in data_type:
            return 'NUMERIC'
        elif 'bool' in data_type:
            return 'BOOL'
        elif 'date' in data_type:
            return 'DATE'
        elif 'time' in data_type:
            return 'TIME'
        elif 'float' in data_type:
            return 'FLOAT64'
        elif 'bytea' in data_type:
            return 'BYTES'
        elif 'record' in udt_name:
            # Extract the nested fields of the record array
            record_fields = udt_name[udt_name.find('(') + 1:udt_name.rfind(')')]
            nested_schema = []
            for field in record_fields.split(','):
                field_name, field_type = field.strip().split(':')
                field_schema = bigquery.SchemaField(field_name.strip(), convert_pgadmin_to_bigquery(field_type.strip()))
                nested_schema.append(field_schema)
            return 'RECORD' + str(nested_schema)
        else:
            return 'STRING'
    else:
        return 'STRING'


# Function to get schema fields from catalog


# def get_schema_fields_from_catalog(stream_index):
#     with open('catalog.json', 'r') as file:
#         catalog_data = json.load(file)
#     property_array = catalog_data['streams'][stream_index]['metadata']
#     schema_fields = []
#     for property_obj in property_array:
#         property_name = property_obj['breadcrumb'][1]
#         data_type = property_obj['metadata']['type']
#         data_type = convert_pgadmin_to_bigquery(data_type)
#         schema_fields.append(bigquery.SchemaField(property_name, data_type))
#     return schema_fields


jsonb_columns = {""}
def get_schema_fields_from_catalog2(stream_index):
    with open('catalog.json', 'r') as file:
        catalog_data = json.load(file)
    property_array = catalog_data['streams'][stream_index]['metadata']
    schema_fields = []
    for property_obj in property_array:
        # print(property_obj)
        property_name = property_obj['breadcrumb'][1]
        data_type = property_obj['metadata']['type']
        udt_name = property_obj['metadata']['udt_name']

        if data_type == 'ARRAY':  # array-like type
            inner_type = convert_pgadmin_to_bigquery(data_type,udt_name)  # extract inner type recursively
            schema_fields.append(bigquery.SchemaField(property_name, inner_type, mode='REPEATED'))
        elif data_type == 'jsonb' or data_type == 'json':
            schema_fields.append(bigquery.SchemaField(property_name, 'STRING', mode='REPEATED'))
            jsonb_columns.add(property_name)
        else:
            data_type = convert_pgadmin_to_bigquery(data_type,"")
            schema_fields.append(bigquery.SchemaField(property_name, data_type))
    return schema_fields


# Function to connect to BigQuery
def connect_to_bigquery(config):
    keyfile_path = config['keyfile_path']
    project_id = config['project_id']
    dataset_id = config['dataset_id']
    client = bigquery.Client.from_service_account_json(keyfile_path)
    dataset_ref = client.dataset(dataset_id)
    return client, dataset_ref


# Function to load records into BigQuery table
# Working code with flattening
def load_records_to_bigquery(client, dataset_ref, table, records, stream_index):
    # Flatten nested fields in records
    flattened_records = []
    # for record in records:
    #     flattened_record = flatten_nested_fields(record)
    #     flattened_records.append(flattened_record)
    flattened_records = records

    table_ref = dataset_ref.table(table)
    temp_table_id = f"temp_table_{table}"
    temp_table_ref = dataset_ref.table(temp_table_id)

    primary_key_fields = ['id']  # Primary key field(s)


    try:
        bq_table = client.get_table(table_ref)
        schema = bq_table.schema
    except NotFound:
        schema_fields = get_schema_fields_from_catalog2(stream_index)
        table = bigquery.Table(table_ref, schema=schema_fields)
        bq_table = client.create_table(table)
        schema = bq_table.schema
    
    # creating temporary table if not exists
    try:
        temp_table = client.get_table(temp_table_ref)
        # schema = bq_table.schema
    except NotFound:
        schema_fields = get_schema_fields_from_catalog2(stream_index)
        temp_table = bigquery.Table(temp_table_ref, schema=schema_fields)
        temp_table = client.create_table(temp_table,exists_ok=True)
        # schema = bq_table.schema

    # Calculate expiration time for the temporary table
    expiration_time = datetime.datetime.now(pytz.UTC) + datetime.timedelta(minutes=1)

    # Convert expiration_time to Unix timestamp
    expiration_timestamp = int(expiration_time.timestamp())

    # Convert datetime objects to ISO 8601 formatted strings
    for record in records:
        for field in schema:
            # print(field)
            if field.field_type == 'TIMESTAMP':
                field_name = field.name
                if field_name in record:
                    value = record[field_name]
                    if isinstance(value, datetime.datetime):
                        record[field_name] = value.isoformat()
            if field.name in jsonb_columns and field.name in record:
                print(record[field.name])
                json_string = json.dumps(record[field.name])
                print(json_string)
                record[field.name] = json_string


    batch_size = 1000
    for i in range(0, len(flattened_records), batch_size):
        batch_records = flattened_records[i:i + batch_size]

        # Insert rows into temporary table
        if temp_table:
            logging.info(f"Syncing records for stream: {table_ref.table_id}")
            errors = client.insert_rows(temp_table_ref, batch_records, selected_fields=schema)
            logging.info("Last sync time: {}".format(datetime.datetime.now()))


        if errors:
            print(errors)
            for error in errors:
                print(error)
                error_message = error.get('message', 'Unknown error')
                singer.write_record(table, {"error": error_message})


    print("==================================================================================")
    print(f'''`{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}`''')
    print(f'''`{temp_table_ref.project}.{temp_table_ref.dataset_id}.{temp_table_ref.table_id}`''')
    print("==================================================================================")

    merge_query = f"""
        MERGE `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` AS T
        USING (
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY {', '.join(primary_key_fields)} ORDER BY {', '.join(primary_key_fields)}) AS row_number
                FROM `{temp_table_ref.project}.{temp_table_ref.dataset_id}.{temp_table_ref.table_id}`
            )
            WHERE row_number = 1
        ) AS S
        ON {f"{' AND '.join(f'T.{field} = S.{field}' for field in primary_key_fields)}"}
        WHEN MATCHED THEN
            UPDATE SET {', '.join(f"T.{field.name} = S.{field.name}" for field in schema if field.name not in primary_key_fields)}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(field.name for field in schema)})
            VALUES ({', '.join(f"S.{field.name}" for field in schema)})
    """

    query_job = client.query(merge_query)
    query_job.result()


    print()
    print("==================================================================================================")

    print("Data loaded successfully.")
    expiration_job = client.delete_table(temp_table_ref, not_found_ok=True)
    # expiration_job.result()


# Main function
def main():
    args = singer.utils.parse_args(required_config_keys=[])
    config = args.config

    client, dataset_ref = connect_to_bigquery(config)

    stream_name = ""
    stream_index = -1
    records = []

    date_format = '%Y-%m-%dT%H:%M:%S.%f'

    # Get start time
    start_time = datetime.datetime.now()
    logging.info(f"Sync started at: {start_time}")


    for line in sys.stdin:
        message = json.loads(line)
        if message['type'] == 'RECORD':
            if message['stream'] != stream_name:
                if stream_name != "":
                    # Load any remaining records for the previous stream
                    logging.info(f"Loading records into BigQuery table: {stream_name}")
                    load_records_to_bigquery(client, dataset_ref, stream_name, records, stream_index)
                    logging.info("Last sync time: {}".format(datetime.datetime.now()))

                stream_name = message['stream']
                stream_index += 1
                records = []

            table = message['stream']
            record = message['record']


            records.append(record)

    if stream_name != "":
        logging.info(f"Loading records into BigQuery table: {stream_name}")
        # Load any remaining records for the last stream
        load_records_to_bigquery(client, dataset_ref, table, records, stream_index)
        logging.info("Last sync time: {}".format(datetime.datetime.now()))

    # Get end time
    end_time = datetime.datetime.now()
    logging.info(f"Sync ended at: {end_time}")

    # Calculate and log the program execution time
    execution_time = end_time - start_time
    logging.info(f"Sync execution time: {execution_time}")

    print("==================================================================================================")
    client.close()


if __name__ == '__main__':
    main()

