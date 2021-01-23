import json

import jsonschema
from jsonschema import validate


def get_schema():
    """
    This function returns the json config schema
    :return: Validation schema
    """
    with open('src/config_schema.json', 'r') as file:
        schema = json.load(file)
    return schema


def validate_cfg(json_data):
    """
    This function validates config object against config schema
    :param json_data: config object
    :return: Validation status and message: True if config object is valid, otherwise False
    """

    # Load validation schema
    execute_api_schema = get_schema()

    # Validation process
    try:
        validate(instance=json_data, schema=execute_api_schema)
    except jsonschema.exceptions.ValidationError as err:
        return False, err

    message = "Given JSON data is Valid"
    return True, message
