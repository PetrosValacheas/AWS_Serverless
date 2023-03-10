from unittest.mock import patch
from lambdaFunctions.API.aggregateEnergyProduction.aggregateEnergyProduction import get_energy_production


def mock_park(park_id):
    park = {
        "energy_type": "Wind" if park_id == "park1" else "Solar"
    }
    return park


@patch('lambdaFunctions.API.aggregateEnergyProduction.aggregateEnergyProduction.retrieve_park', mock_park)
def test_get_energy_production_wind_energy():
    energy_data = {
        "park1": [{"energy_value": 100}, {"energy_value": 200}],
        "park2": [{"energy_value": 300}]
    }
    energy_production = get_energy_production(energy_data)
    assert energy_production == {'solar': {'production': 300, 'units': 'MWh'},
                                 'wind': {'production': 300, 'units': 'MWh'}}


def test_get_energy_production_no_data():
    energy_data = {}
    with patch('lambdaFunctions.API.aggregateEnergyProduction.aggregateEnergyProduction.retrieve_park', mock_park):
        energy_production = get_energy_production(energy_data)
        assert energy_production == {}


def test_get_energy_production_invalid_data():
    energy_data = {
        "park1": [{"energy_value": 100}, {"energy_value": 0}],
        "park2": [{"energy_value": 300}]
    }
    with patch('lambdaFunctions.API.aggregateEnergyProduction.aggregateEnergyProduction.retrieve_park', mock_park):
        energy_production = get_energy_production(energy_data)
        assert energy_production == {'solar': {'production': 300, 'units': 'MWh'}, 'wind': {'production': 100, 'units': 'MWh'}}
