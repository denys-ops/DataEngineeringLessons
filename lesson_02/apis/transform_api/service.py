import json
import os
from fastavro import writer, parse_schema
avro_schema = {
        "type": "record",
        "name": "Purchase",
        "fields": [
            {
                "name": "client",
                "type": "string"
            },
            {
                "name": "purchase_date",
                "type": "string",
            },
            {
                "name": "product",
                "type": "string"
            },
            {
                "name": "price",
                "type": "int"
            }
        ]
}

parsed_schema = parse_schema(avro_schema)


def convert_json_to_avro(json_directory, avro_directory):
    """Convert all files from JSON directory to AVRO with same names."""
    # Check if directory exists
    if not os.path.exists(avro_directory):
        os.makedirs(avro_directory)

    # Transform all files
    for file_name in os.listdir(json_directory):
        if file_name.endswith('.json'):
            json_file_path = os.path.join(json_directory, file_name)
            avro_file_path = os.path.join(avro_directory, file_name.replace('.json', '.avro'))

            # Read JSON
            with open(json_file_path, 'r', encoding='utf-8') as json_file:
                records = json.load(json_file)

            # Write AVRO
            with open(avro_file_path, 'wb') as avro_file:
                writer(avro_file, parsed_schema, records)
