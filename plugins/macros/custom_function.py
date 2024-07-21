from enum import Enum
from airflow.models.variable import Variable


def fetch_all_variables():
    """Fetch all registered Airflow variables."""
    return Variable.get_all()


class AirflowVariables(Enum):
    """Enum class to store all Airflow variables."""

    pass


def create_airflow_variables_enum():
    """Create an Enum from all Airflow variables."""
    variables = fetch_all_variables()
    return Enum("AirflowVariables", variables)
