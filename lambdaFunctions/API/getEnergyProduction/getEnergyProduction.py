import boto3
from layers.utils.athena import Athena, get_athena_configuration, get_athena_query
from layers.utils.utils import aws_response
from typing import Tuple, List

ssm = boto3.client("ssm")

STAGE = "production"


def load_event_params(event) -> Tuple[List[str], int, int]:
    params = event.get("queryStringParameters", {})
    park_ids = params.get("park_ids", [])
    start_timestamp = params.get("start_timestamp", None)
    end_timestamp = params.get("end_timestamp", None)

    return park_ids, start_timestamp, end_timestamp


def lambda_handler(event, context):
    park_ids, start_timestamp, end_timestamp = load_event_params(event=event)

    if not park_ids:
        print(f"No Park Ids provided!")
        return aws_response({"message": "No Park Ids provided"}, 400)

    if not start_timestamp:
        print(f"No start timestamp provided!")
        return aws_response({"message": "No start timestamp provided"}, 400)

    if not end_timestamp:
        print(f"No start timestamp provided!")
        return aws_response({"message": "No end timestamp provided"}, 400)

    athena = Athena()
    athena_configuration = get_athena_configuration(ssm, STAGE)
    print(f"Athena configuration: {athena_configuration}")

    athena_query = get_athena_query(athena, park_ids, int(start_timestamp), int(end_timestamp), athena_configuration)
    if not athena_query:
        return 500

    response = athena_query.format_paginated_query_results(athena_configuration["max_results"])
    print(f"Sending Energy data: {response}")
    return aws_response(response, 200)
