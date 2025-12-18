"""
Blueprint template for scheduling Jupyter Notebook execution.

This template allows users to schedule parameterized Jupyter notebooks
to run on a recurring schedule. Ideal for data scientists and analysts
who want to automate their notebook-based analyses.
"""

from datetime import datetime, timedelta
from typing import Optional, Any
from pydantic import BaseModel, Field
from blueprint import Blueprint
from airflow.decorators import dag, task
from airflow.providers.papermill.operators.papermill import PapermillOperator


class JupyterNotebookConfig(BaseModel):
    """Configuration model for Jupyter notebook scheduling."""

    dag_id: str = Field(
        ...,
        description="Unique identifier for this DAG (e.g., 'monthly_player_analysis')"
    )

    description: str = Field(
        ...,
        description="Brief description of what this notebook does"
    )

    schedule: str = Field(
        ...,
        description="Cron expression or preset (e.g., '@weekly', '0 9 * * MON')"
    )

    notebook_path: str = Field(
        ...,
        description="Path to the input notebook (e.g., 'include/notebooks/analysis.ipynb')"
    )

    output_path: str = Field(
        ...,
        description="Path where executed notebook will be saved (e.g., 'include/notebooks/output/analysis_{{ ds }}.ipynb')"
    )

    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters to pass to the notebook (accessible via papermill)"
    )

    owner: str = Field(
        default="data_science_team",
        description="Owner of this DAG"
    )

    retries: int = Field(
        default=1,
        ge=0,
        le=5,
        description="Number of retries on failure (0-5)"
    )

    retry_delay_minutes: int = Field(
        default=10,
        ge=1,
        le=60,
        description="Minutes to wait between retries"
    )

    tags: list[str] = Field(
        default_factory=lambda: ["jupyter", "notebook", "analytics"],
        description="Tags to categorize this DAG"
    )

    start_date: Optional[datetime] = Field(
        default=None,
        description="When to start the DAG (defaults to 2024-01-01)"
    )

    kernel_name: str = Field(
        default="python3",
        description="Jupyter kernel to use for execution"
    )


class JupyterNotebookBlueprint(Blueprint[JupyterNotebookConfig]):
    """
    Blueprint for creating scheduled Jupyter notebook DAGs.

    This template makes it easy to schedule recurring notebook executions
    with parameterization. Perfect for data scientists who want to automate
    their analyses, generate reports, or run ML model evaluations on a schedule.
    """

    def generate_dag(self, config: JupyterNotebookConfig):
        """Generate an Airflow DAG from the configuration."""

        @dag(
            dag_id=config.dag_id,
            description=config.description,
            schedule=config.schedule,
            start_date=config.start_date or datetime(2024, 1, 1),
            catchup=False,
            tags=config.tags,
            default_args={
                "owner": config.owner,
                "retries": config.retries,
                "retry_delay": timedelta(minutes=config.retry_delay_minutes),
            },
        )
        def jupyter_notebook_dag():
            """Execute a scheduled Jupyter notebook."""

            # Execute the notebook with papermill
            execute_notebook = PapermillOperator(
                task_id="execute_notebook",
                input_nb=config.notebook_path,
                output_nb=config.output_path,
                parameters=config.parameters,
                kernel_name=config.kernel_name,
            )

            @task
            def log_completion():
                """Log notebook execution completion."""
                print(f"Notebook execution completed: {config.output_path}")
                return f"Success: {config.dag_id}"

            execute_notebook >> log_completion()

        return jupyter_notebook_dag()


# Register the blueprint
blueprint = JupyterNotebookBlueprint()
