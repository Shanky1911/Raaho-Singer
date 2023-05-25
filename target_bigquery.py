import singer
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import json
import sys
import psycopg2
import datetime
from google.cloud import bigquery

def convert_pgadmin_to_bigquery(data_type):
    data_type = data_type.lower()
    if data_type.startswith('int'):
        return 'INT64'
    elif data_type.startswith('serial') or data_type.startswith('bigserial'):
        return 'INT64'
    elif data_type.startswith('bigint'):
        return 'INT64'
    elif data_type.startswith('smallint'):
        return 'INT64'
    elif data_type.startswith('numeric') or data_type.startswith('decimal'):
        return 'NUMERIC'
    elif data_type.startswith('float') or data_type.startswith('double'):
        return 'FLOAT64'
    elif data_type.startswith('real'):
        return 'FLOAT64'
    elif data_type.startswith('boolean'):
        return 'BOOL'
    elif data_type.startswith('character') or data_type.startswith('char') or data_type.startswith('bpchar'):
        return 'STRING'
    elif data_type.startswith('text') or data_type.startswith('citext'):
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
    else:
        return 'STRING'
    


def get_schema_fields_from_catalog(stream_index):
    with open('catalog.json', 'r') as file:
        catalog_data = json.load(file)
    property_array = catalog_data['streams'][stream_index]['metadata']
    schema_fields = []
    for property_obj in property_array:
        property = property_obj['breadcrumb'][1]
        data_type = property_obj['metadata']['type']
        # data_type = convert_pgadmin_to_bigquery(data_type)
        if(data_type == 'character varying'):
            data_type = 'STRING'
        elif(data_type == 'timestamp without time zone'):
            data_type = "TIMESTAMP"
        schema_fields.append(bigquery.SchemaField(property, data_type))
    return schema_fields


def connect_to_bigquery(config):
    keyfile_path = config['keyfile_path']
    project_id = config['project_id']
    dataset_id = config['dataset_id']
    client = bigquery.Client.from_service_account_json(keyfile_path)
    dataset_ref = client.dataset(dataset_id)
    return client, dataset_ref


def record_exists(client, table_ref, record):
    # Constructing a query to check if the record already exists in BigQuery
    record_id = record['id']
    query = f"SELECT COUNT(*) FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` WHERE id = {record_id}"
    query_job = client.query(query)
    
    result = query_job.result()
    row_count = next(result)[0]
    return row_count


def load_record_to_bigquery(client, dataset_ref, table, record,stream_index):
    table_ref = dataset_ref.table(table)
    
    try:
        bq_table = client.get_table(table_ref)
        schema = bq_table.schema
    except NotFound:

        # Defining the schema fields
        schema_fields = get_schema_fields_from_catalog(stream_index)

        # Creating the table with the specified schema
        table = bigquery.Table(table_ref, schema=schema_fields)
        bq_table = client.create_table(table)
        schema = bq_table.schema

    record_count = record_exists(client,table_ref,record)
    if not record_count >= 1:

        # Convert datetime objects to ISO 8601 formatted strings
        for field in schema:
            if field.field_type == 'TIMESTAMP':
                field_name = field.name
                if field_name in record:
                    value = record[field_name]
                    if isinstance(value, datetime.datetime):
                        record[field_name] = value.isoformat()
        
        # Loading the rows into BigQuery
        if bq_table:
            errors = client.insert_rows(table_ref, [record], selected_fields=schema)

        if errors:
            for error in errors:
                print(error)
                error_message = error.get('message', 'Unknown error')
                singer.write_record(table, {"error": error_message})
        else:
            # Convert the datetime to string representation before writing state
            last_synced_at = singer.utils.now().isoformat()
            singer.write_state({"last_synced_at": last_synced_at})


def main():
    args = singer.utils.parse_args(required_config_keys=[])
    config = args.config

    client, dataset_ref = connect_to_bigquery(config)

    stream_name = ""
    stream_index = -1

    for line in sys.stdin:
        message = json.loads(line)
        # print(message)
        if message['type'] == 'RECORD':
            if message['stream'] != stream_name:
                stream_index += 1
                stream_name = message['stream']
            table = message['stream']
            record = message['record']
            load_record_to_bigquery(client, dataset_ref, table, record,stream_index)
    
    client.close()


if __name__ == '__main__':
    main()
