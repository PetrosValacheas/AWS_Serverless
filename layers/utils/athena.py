import time
import boto3
import json
from datetime import datetime

from typing import Union, Tuple


class Athena:
    class _Query:

        def __init__(self, query_id: str, athena_client, athena_configuration: dict):
            self.query_id = query_id
            self.athena_client = athena_client
            self._status = {}
            self.athena_configuration = athena_configuration

        @property
        def status(self):
            if self._status.get("value") in self.athena_configuration["query_statuses"]["final"]:
                return self._status

            self._status["value"], self._status["reason"] = self._get_query_status()
            return self._status

        def _get_query_status(self) -> Tuple[str, str]:
            response = self.athena_client.get_query_execution(QueryExecutionId=self.query_id)
            status = response['QueryExecution']['Status']['State']
            reason = response['QueryExecution']['Status'].get('StateChangeReason', '')
            return status, reason

        def format_paginated_query_results(self, max_results: Union[int, None] = 100):
            """
            Yield batches of results, as paginated by boto3 client using the MaxResults.
            """
            query_completed = False
            header = []
            formatted_results = {}
            request_arguments = {
                "QueryExecutionId": self.query_id
            }
            if max_results:
                request_arguments["MaxResults"] = max_results

            while not query_completed:
                response = self.athena_client.get_query_results(**request_arguments)
                next_token = response.get('NextToken')
                query_completed = next_token is None
                request_arguments['NextToken'] = next_token
                if not header:
                    # The first Row of the ResultSet contains the column names
                    header = parse_query_result_metadata(response['ResultSet']['ResultSetMetadata'])
                print(f"Fetching another {len(response['ResultSet']['Rows'][1:])} results")
                for row in response['ResultSet']['Rows'][1:]:
                    formatted_row = parse_query_result_row(row['Data'], header)
                    for park_id, rows in formatted_row.items():
                        if park_id not in formatted_results:
                            formatted_results[park_id] = []
                        formatted_results[park_id].extend(rows)
            return formatted_results

        def get_all_query_results(self) -> dict:
            results = {}
            for park_id, batch in self.get_paginated_query_results(max_results=None):
                if park_id not in results:
                    results[park_id] = []
                results[park_id].extend(batch)
            return results

        def poll_for_status(self) -> dict:
            while self.status["value"] not in self.athena_configuration["query_statuses"]["final"]:
                print(f"Waiting for query execution to complete, sleep for "
                      f"{self.athena_configuration['query_status_poll_interval_seconds']}")
                time.sleep(self.athena_configuration["query_status_poll_interval_seconds"])

            print(f"Query status {self.status['value']} Reason: {self.status['reason']}")
            return self.status

    def __init__(self, stage="production"):
        self.ssm = boto3.client("ssm")
        self.stage = stage
        self.query_result_bucket = self._get_query_result_location()
        self.configuration = get_athena_configuration(self.ssm, self.stage)
        self.athena_client = boto3.client("athena", region_name=self.configuration["database_region"])

    def _get_query_result_location(self) -> str:
        return "s3://energy-production-athena"

    def query(self, query_string: str, database: str) -> _Query:
        response = self.athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={
                'OutputLocation': self.query_result_bucket,
                'EncryptionConfiguration': {'EncryptionOption': "SSE_S3"}}
        )
        return Athena._Query(response['QueryExecutionId'], self.athena_client, self.configuration)


def parse_query_result_row(row: list, metadata: list) -> dict:
    formatted_row = {}
    current_park_id = ""

    for index, value in enumerate(row):
        column_name = metadata[index]['name']

        if column_name == 'park_id':
            current_park_id = value['VarCharValue']
            if current_park_id not in formatted_row:
                formatted_row[current_park_id] = []
        elif column_name.startswith('timestamp'):
            timestamp = datetime.strptime(value['VarCharValue'], '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%SZ')
            formatted_row[current_park_id].append({'timestamp': timestamp})
        elif column_name.startswith('energy_value'):
            energy_value = float(value['VarCharValue'])
            formatted_row[current_park_id][-1]['energy_value'] = energy_value

    return formatted_row


def parse_query_result_metadata(metadata: dict) -> list:
    """
    Parse the metadata of a query and return all column names and types. The input metadata (as returned by boto3)
    is formatted as:
    {'ColumnInfo':
        [
            {
            'Name': "name of the column",
            'Type': "data type of the column",
            ...
            }
        ]
    }
    """
    return [{'name': column['Name'], 'type': column['Type']} for column in metadata['ColumnInfo']]


def get_athena_configuration(ssm, stage: str) -> dict:
    athena_configuration = json.loads(ssm.get_parameter(Name="athena_config").get("Parameter", "").get("Value", ""))
    return athena_configuration.get(stage, {})


def format_query_string(park_ids: list, start: int, end: int) -> str:
    start_dt = datetime.utcfromtimestamp(start / 1000).strftime('%Y-%m-%d %H:%M:%S')
    end_dt = datetime.utcfromtimestamp(end / 1000).strftime('%Y-%m-%d %H:%M:%S')
    park_table_names = [f"{park_id} AS p{i}" for i, park_id in enumerate(park_ids)]
    park_table_names_str = ",".join(park_table_names)
    select_columns = ", ".join([f"'{park_id}' AS park_id, p{i}.timestamp AS timestamp{i}, p{i}.energy_value AS energy_value{i}" for i, park_id in enumerate(park_ids)])
    where_clauses = " AND ".join([
                                     f"CAST(p{i}.timestamp AS timestamp) >= CAST('{start_dt}' AS timestamp) AND CAST(p{i}.timestamp AS timestamp) <= CAST('{end_dt}' AS timestamp)"
                                     for i in range(len(park_ids))])
    order_by_columns = ", ".join([f"timestamp{i}" for i in range(len(park_ids))])
    return f"""SELECT {select_columns}
                FROM {park_table_names_str}
                WHERE {where_clauses}
                ORDER BY {order_by_columns} ASC"""


def get_athena_query(athena: Athena, park_ids: list, start: int, end: int, athena_configuration: dict):
    query_str = format_query_string(park_ids=park_ids,
                                    start=start, end=end)

    query = athena.query(query_str, athena_configuration['database'])

    if query.poll_for_status() in athena_configuration['query_statuses']['failed']:
        print(f"Query Failed! Status {query.status['value']} Reason: {query.status['reason']}")
        return

    return query
