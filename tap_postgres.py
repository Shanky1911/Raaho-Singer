import singer
import psycopg2
import json
import datetime

REQUIRED_CONFIG_KEYS = ['host', 'port', 'user', 'password', 'database', 'tables']

BATCH_SIZE = 1000 # Adjust batch size as needed

def connect_to_postgres(config):
    conn = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )
    return conn


def discover(conn, config):
    tables = config['tables']
    schemas = config.get('schemas')
    catalog = {'streams': []}

    cursor = conn.cursor()

    for table in tables:
        stream = {
            'tap_stream_id': table,
            'stream': table,
            'schema': schemas.get(table) if schemas else None,
            'metadata': [],
            'key_properties': []
        }
        
        cursor.execute(f"SELECT column_name, data_type,udt_name FROM information_schema.columns WHERE table_name = '{table}'")
        columns = cursor.fetchall()
        # print(columns)

        for column_name, data_type,udt_name in columns:
            stream['metadata'].append({'breadcrumb': ['properties', column_name], 'metadata': {'selected-by-default': True,'udt_name':udt_name}})
            if column_name in config.get('primary_key_fields', []):
                stream['key_properties'].append(column_name)
            stream['metadata'][-1]['metadata']['type'] = data_type
        catalog['streams'].append(stream)

    json_object = json.dumps(catalog, indent=4)
    with open('catalog.json', 'w') as catalog_file:
        catalog_file.write(json_object)

    cursor.close()
    return catalog


def fetch_records(cursor, table, schema):
    query = f"SELECT * FROM {table} "
    if schema:
        query = f"SELECT * FROM {schema}.{table}"

    current_time = datetime.datetime.now()
    one_hour_ago = current_time - datetime.timedelta(hours=1)
    
    # Add a condition to the query to filter records updated in the last one hour
    # query += f"WHERE updated_at >= '{one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')}'"

    cursor.execute(query)
    rows = cursor.fetchmany(BATCH_SIZE)
    while rows:
        yield rows
        rows = cursor.fetchmany(BATCH_SIZE)



def sync_table(conn, table, schema):
    cursor = conn.cursor()

    singer.write_schema(table, {'properties': {}}, 'id')

    for rows in fetch_records(cursor, table, schema):
        records = []
        for row in rows:
            record = dict(zip([desc[0] for desc in cursor.description], row))
            for key, value in record.items():
                if isinstance(value, datetime.datetime):
                    record[key] = value.isoformat()  # Convert datetime to string
            records.append(record)

        singer.write_records(table, records)

    cursor.close()


def sync(config):
    conn = connect_to_postgres(config)

    for table in config['tables']:
        schema = config.get('schemas', {}).get(table)
        sync_table(conn, table, schema)

    conn.close()


def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    conn = connect_to_postgres(config)
    # catalog = discover2(conn, config)
    catalog = discover(conn, config)
    conn.close()

    for stream in catalog['streams']:
        # print(stream)
        singer.write_schema(stream['stream'], {'properties': {}}, stream['key_properties'])

    sync(config)


if __name__ == '__main__':
    main()
