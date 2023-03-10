import unittest
from moto import mock_athena

from layers.utils.athena import (
    Athena,
    parse_query_result_metadata,
    parse_query_result_row,
    format_query_string
)


class TestAthena(unittest.TestCase):

    @mock_athena
    def test_query(self):
        athena = Athena()
        query_str = "SELECT 1"
        database = athena.configuration["database"]
        query = athena.query(query_str, database)
        self.assertIsInstance(query, Athena._Query)

    def test_parse_query_result_metadata(self):
        metadata = {
            "ColumnInfo": [
                {"Name": "col1", "Type": "varchar"},
                {"Name": "col2", "Type": "int"},
            ]
        }
        expected_output = [
            {"name": "col1", "type": "varchar"},
            {"name": "col2", "type": "int"},
        ]
        output = parse_query_result_metadata(metadata)
        self.assertEqual(output, expected_output)

    def test_parse_query_result_row(self):
        row = {
            "Data": [
                {"VarCharValue": "park1"},
                {"VarCharValue": "2022-05-01 00:00:00.000"},
                {"VarCharValue": "100"},
            ]
        }
        metadata = [
            {"name": "park_id", "type": "varchar"},
            {"name": "timestamp", "type": "timestamp"},
            {"name": "energy_value", "type": "double"},
        ]
        expected_output = {"park1": [{"timestamp": "2022-05-01T00:00:00Z", "energy_value": 100.0}]}
        output = parse_query_result_row(row["Data"], metadata)
        self.assertEqual(output, expected_output)

    def test_parse_query_result_row(self):
        row = {
            'park_id': {'VarCharValue': 'park1'},
            'timestamp0': {'VarCharValue': '2022-03-11 20:18:02.000'},
            'energy_value0': {'VarCharValue': '10.0'},
            'timestamp1': {'VarCharValue': '2022-03-11 20:18:05.000'},
            'energy_value1': {'VarCharValue': '20.0'},
        }
        metadata = [
            {'name': 'park_id', 'type': 'varchar'},
            {'name': 'timestamp0', 'type': 'varchar'},
            {'name': 'energy_value0', 'type': 'varchar'},
            {'name': 'timestamp1', 'type': 'varchar'},
            {'name': 'energy_value1', 'type': 'varchar'},
        ]
        expected_result = {
            'park1': [
                {'timestamp': '2022-03-11T20:18:02Z', 'energy_value': 10.0},
                {'timestamp': '2022-03-11T20:18:05Z', 'energy_value': 20.0},
            ],
        }
        assert parse_query_result_row(row, metadata) == expected_result

    def test_format_query_string(self):
        expected_result = "SELECT 'park1' AS park_id, p0.timestamp AS timestamp0, p0.energy_value AS energy_value0, 'park2' AS park_id, p1.timestamp AS timestamp1, p1.energy_value AS energy_value1 FROM park1 AS p0, park2 AS p1 WHERE CAST(p0.timestamp AS timestamp) >= CAST('2022-03-13 00:38:02' AS timestamp) AND CAST(p0.timestamp AS timestamp) <= CAST('2022-03-13 01:38:02' AS timestamp) AND CAST(p1.timestamp AS timestamp) >= CAST('2022-03-13 00:38:02' AS timestamp) AND CAST(p1.timestamp AS timestamp) <= CAST('2022-03-13 01:38:02' AS timestamp) ORDER BY timestamp0, timestamp1 ASC"

        park_ids = ['park1', 'park2']
        start = 1647131882000
        end = 1647135482000

        assert format_query_string(park_ids, start, end) == expected_result

