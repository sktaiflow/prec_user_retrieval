from airflow.models.variable import Variable

from .utils.enum import StrEnum
from typing import Dict

DEFAULT_VARIABLES = {
    "ENV": "stg",
    "GCP_PROJECT_ID": "skt-datahub",
    "DEBUG": "False",
    "MAX_RETRIES": "3",
    "slack_conn_id": "slack_conn",
    "nudge_api_token": None,
    # Add more default variables as needed
}


def fetch_variables(var_dict: Dict[str, str]) -> Dict[str, str]:
    """Fetch all registered Airflow variables."""
    for key in var_dict.keys():
        val = Variable.get(key)
        var_dict[key] = val
    return var_dict


def create_airflow_variables_enum(var_dict=DEFAULT_VARIABLES):
    """Create an Enum from merged Airflow and default variables. [priority: Airflow > Default variables]"""
    airflow_vars = fetch_variables(var_dict=var_dict)
    return StrEnum("AirflowVariables", airflow_vars)


class AirflowVariables(StrEnum):
    """Enum class to store all Airflow variables."""

    pass
