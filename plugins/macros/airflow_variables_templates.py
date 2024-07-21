from airflow.models.variable import Variable
from enum import Enum
from typing import Dict


class StrEnum(str, Enum):
    def _generate_next_value_(self, start, count, last_values):
        return self

    def __repr__(self) -> str:
        return self.value

    def __str__(self) -> str:
        return self.value

    @classmethod
    def list_values(cls) -> list[str]:
        return [e.value for e in cls]

    @classmethod
    def list_names(cls) -> list[str]:
        return [e.name for e in cls]


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


def create_airflow_variables_enum(vars: DefaultVariables) -> StrEnum:
    """Create an Enum from merged Airflow and default variables. [priority: Airflow > Default variables]"""
    airflow_vars = fetch_variables(vars=vars)
    return StrEnum("AirflowVariables", airflow_vars)


class AirflowVariables(StrEnum):
    """Enum class to store all Airflow variables."""

    pass
