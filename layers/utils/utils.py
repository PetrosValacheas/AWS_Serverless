import json
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
parks_table = dynamodb.Table("Parks")


def aws_response(message, status_code):
    """
    Helper function to construct aws_responses
    """
    return {
        "body": json.dumps(message),
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
    }


def retrieve_park(park_id: str) -> dict:
    parks = parks_table.query(
        KeyConditionExpression=Key("park_id").eq(park_id)
    )
    return parks["Items"][0] if parks["Items"] else {}
