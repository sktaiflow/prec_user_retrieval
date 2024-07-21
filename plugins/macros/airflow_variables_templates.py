from enum import Enum
from airflow.models.variable import Variable
from utils.enum import StrEnum


DEFAULT_VARIABLES = {
    "ENV": "stg",
    "GCP_PROJECT_ID": "skt-datahub",
    "DEBUG": "False",
    "MAX_RETRIES": "3",
    # Add more default variables as needed
}


def merge_variables(airflow_vars, default_vars):
    """Merge Airflow variables with default variables."""
    merged = default_vars.copy()
    merged.update(airflow_vars)
    return merged


def fetch_all_variables():
    """Fetch all registered Airflow variables."""
    return Variable.get_all()


def create_airflow_variables_enum():
    """Create an Enum from merged Airflow and default variables. [priority: Airflow > Default variables]"""
    airflow_vars = fetch_all_variables()
    merged_vars = merge_variables(airflow_vars, DEFAULT_VARIABLES)
    return StrEnum("AirflowVariables", merged_vars)


class AirflowVariables(StrEnum):
    """Enum class to store all Airflow variables."""

    pass
