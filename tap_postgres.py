import singer
import psycopg2
import json
import datetime

REQUIRED_CONFIG_KEYS = ['host', 'port', 'user', 'password', 'database', 'tables']

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

        cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
        columns = cursor.fetchall()

        for column_name, data_type in columns:
            stream['metadata'].append({'breadcrumb': ['properties', column_name], 'metadata': {'selected-by-default': True}})
            if column_name in config.get('primary_key_fields', []):
                stream['key_properties'].append(column_name)
            stream['metadata'][-1]['metadata']['type'] = data_type
        catalog['streams'].append(stream)

    json_object = json.dumps(catalog, indent=4)
    with open('catalog.json', 'w') as catalog_file:
        json.dump(catalog,catalog_file)

    cursor.close()
    return catalog

def sync_table(conn, table, schema):
    cursor = conn.cursor()

    query = f"SELECT * FROM {table}"
    if schema:
        query = f"SELECT * FROM {schema}.{table}"

    cursor.execute(query)
    rows = cursor.fetchall()

    singer.write_schema(table, {'properties': {}},'id')
    for row in rows:
        record = dict(zip([desc[0] for desc in cursor.description], row))
        for key, value in record.items():
            if isinstance(value, datetime.datetime):
                record[key] = value.isoformat()  # Convert datetime to string
        singer.write_records(table, [record])
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
    catalog = discover(conn, config)
    conn.close()

    for stream in catalog['streams']:
        singer.write_schema(stream['stream'], {'properties': {}}, stream['key_properties'])

    sync(config)


if __name__ == '__main__':
    main()
