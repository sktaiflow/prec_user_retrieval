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


def fetch_variables(var_dict: Dict[str, str] = DEFAULT_VARIABLES) -> Dict[str, str]:
    """Fetch all registered Airflow variables."""
    for key in DEFAULT_VARIABLES.keys():
        val = Variable.get(key)
        var_dict[key] = val
    return var_dict


def create_airflow_variables_enum():
    """Create an Enum from merged Airflow and default variables. [priority: Airflow > Default variables]"""
    airflow_vars = fetch_variables()
    return StrEnum("AirflowVariables", airflow_vars)


class AirflowVariables(StrEnum):
    """Enum class to store all Airflow variables."""

    pass
