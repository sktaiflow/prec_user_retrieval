from airflow.providers.sktvane.operators.nes import NesOperator

from typing import Dict, Any, List, Union
from textwrap import dedent


def create_nes_task(
    dag,
    task_id: str,
    notebook_path: str,
    notebook_name: str = None,
    parameters: Dict = None,
    doc_md_template: str = "",
    doc_md: Dict = {},
) -> NesOperator:
    """
    Create a NES task.
    Args:
        dag: The DAG object to which the task belongs.
        task_id (str): The unique identifier for the task.
        notebook_path (str): The path to the notebook file.
        notebook_name (str, optional): The name of the notebook file. If not provided, it will be set to "{task_id}.ipynb".
        parameters (Dict, optional): The parameters to be passed to the task.
        doc_md_template (str, optional): The template for the Markdown documentation.
        doc_md (Dict, optional): The dictionary containing the documentation metadata.

    Returns:
        NesOperator: The created NES task.
    """

    if notebook_name is None:
        notebook_name = f"{task_id}.ipynb"

    task = NesOperator(
        task_id=task_id,
        parameters=parameters,
        input_nb=f"{notebook_path}/{notebook_name}",
        dag=dag,
    )
    if isinstance(doc_md, dict) and doc_md:
        task_description = doc_md.get("task_description", "")
        output_tables = doc_md.get("output_tables", "")
        reference_tables = doc_md.get("reference_tables", "")

        doc_md_str = doc_md_template.format(
            task_description=task_description,
            output_tables=output_tables,
            reference_tables=reference_tables,
        )
    else:
        doc_md_str = ""
    task.doc_md = dedent(doc_md_str)
    return task
