import boto3
from layers.utils.athena import Athena, get_athena_configuration, get_athena_query
from layers.utils.utils import aws_response, retrieve_park
from typing import Tuple, List

ssm = boto3.client("ssm")

STAGE = "production"


def load_event_params(event) -> Tuple[List[str], int, int, List[str]]:
    params = event.get("queryStringParameters", {})
    park_ids = params.get("park_ids", [])
    start_timestamp = params.get("start_timestamp", None)
    end_timestamp = params.get("end_timestamp", None)
    energy_types = params.get("energy_types", None)

    return park_ids, start_timestamp, end_timestamp, energy_types


def get_energy_production(energy_data: dict) -> dict:
    wind_energy = 0
    solar_energy = 0
    for park_id, data in energy_data.items():
        park = retrieve_park(park_id)
        energy_type = park.get("energy_type", "")
        if energy_type == "Wind":
            wind_energy += sum([d["energy_value"] for d in data])
        elif energy_type == "Solar":
            solar_energy += sum([d["energy_value"] for d in data])

    energy_production = {}
    if wind_energy > 0:
        energy_production["wind"] = {"production": wind_energy, "units": "MWh"}
    if solar_energy > 0:
        energy_production["solar"] = {"production": solar_energy, "units": "MWh"}

    return energy_production


def lambda_handler(event, context):
    park_ids, start_timestamp, end_timestamp, energy_types = load_event_params(event=event)

    if not park_ids:
        print(f"No Park Ids provided!")
        return aws_response({"message": "No Park Ids provided"}, 400)

    if not start_timestamp:
        print(f"No start timestamp provided!")
        return aws_response({"message": "No start timestamp provided"}, 400)

    if not end_timestamp:
        print(f"No start timestamp provided!")
        return aws_response({"message": "No end timestamp provided"}, 400)

    if not energy_types:
        print(f"No energy types provided!")
        return aws_response({"message": "No energy types provided"}, 400)

    athena = Athena()
    athena_configuration = get_athena_configuration(ssm, STAGE)
    print(f"Athena configuration: {athena_configuration}")

    athena_query = get_athena_query(athena, park_ids, int(start_timestamp), int(end_timestamp), athena_configuration)
    if not athena_query:
        return 500

    energy_data = athena_query.format_paginated_query_results(athena_configuration["max_results"])
    production_output_energy = get_energy_production(energy_data)
    print(f"Production Output Energg: {production_output_energy}")
    return aws_response(production_output_energy, 200)
