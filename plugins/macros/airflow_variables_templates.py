from airflow.models.variable import Variable

from .utils.enum import StrEnum
from typing import Dict


class DefaultVariablesEnum(StrEnum):
    ENV = "stg"
    GCP_PROJECT_ID = "skt-datahub"
    DEBUG = "False"
    MAX_RETRIES = "3"
    SLACK_CONN_PROFILE_ALARMING = "slack_conn"
    NUDGE_API_TOKEN = None
    HDFS_PATH_PIVOT_PROFILE = "/data/adot/jaehwan"


class DefaultVariables:
    def __init__(self):
        self.variables = {var.name: var.value for var in DefaultVariablesEnum}

    def update_variable(self, key, value):
        self.variables[key] = value

    def update_variables_from_dict(self, variables_dict):
        for key, value in variables_dict.items():
            self.update_variable(key, value)
        return self

    def get_variable(self, key):
        return self.variables.get(key, None)


def fetch_variables(vars: DefaultVariables) -> Dict[str, str]:
    """Fetch all registered Airflow variables."""
    for key in vars.variables.keys():
        try:
            val = Variable.get(key)
            vars.update_variable(key, val)
        except KeyError as e:
            pass
    return vars.variables


def create_airflow_variables_enum(vars: DefaultVariables = DefaultVariables()) -> StrEnum:
    """Create an Enum from merged Airflow and default variables. [priority: Airflow > Default variables]"""
    airflow_vars = fetch_variables(vars=vars)
    return StrEnum("AirflowVariables", airflow_vars)


class AirflowVariables(StrEnum):
    """Enum class to store all Airflow variables."""

    pass
