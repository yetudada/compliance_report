"""
Blueprint template for scheduling Snowflake SQL queries.

This template allows users to schedule parameterized Snowflake queries
without writing Python code. Ideal for analysts who need to run recurring
SQL-based reports and analyses.
"""

from datetime import datetime, timedelta
from typing import Optional
from pydantic import BaseModel, Field
from blueprint import Blueprint
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


class SnowflakeQueryConfig(BaseModel):
    """Configuration model for Snowflake query scheduling."""

    dag_id: str = Field(
        ...,
        description="Unique identifier for this DAG (e.g., 'weekly_revenue_report')"
    )

    description: str = Field(
        ...,
        description="Brief description of what this query does"
    )

    schedule: str = Field(
        ...,
        description="Cron expression or preset (e.g., '@daily', '0 0 * * *')"
    )

    sql_query: str = Field(
        ...,
        description="The SQL query to execute in Snowflake"
    )

    snowflake_conn_id: str = Field(
        default="snowflake",
        description="Airflow connection ID for Snowflake"
    )

    owner: str = Field(
        default="analytics_team",
        description="Owner of this DAG"
    )

    retries: int = Field(
        default=2,
        ge=0,
        le=5,
        description="Number of retries on failure (0-5)"
    )

    retry_delay_minutes: int = Field(
        default=5,
        ge=1,
        le=60,
        description="Minutes to wait between retries"
    )

    tags: list[str] = Field(
        default_factory=lambda: ["snowflake", "analytics"],
        description="Tags to categorize this DAG"
    )

    start_date: Optional[datetime] = Field(
        default=None,
        description="When to start the DAG (defaults to 2024-01-01)"
    )


class SnowflakeQueryBlueprint(Blueprint[SnowflakeQueryConfig]):
    """
    Blueprint for creating scheduled Snowflake query DAGs.

    This template makes it easy to schedule recurring SQL queries against
    Snowflake without writing Python code. Perfect for analysts who need
    to run regular reports, data quality checks, or analytics queries.
    """

    def generate_dag(self, config: SnowflakeQueryConfig):
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
        def snowflake_query_dag():
            """Execute a scheduled Snowflake query."""

            SQLExecuteQueryOperator(
                task_id="execute_query",
                conn_id=config.snowflake_conn_id,
                sql=config.sql_query,
                show_return_value_in_logs=True,
            )

        return snowflake_query_dag()


# Register the blueprint
blueprint = SnowflakeQueryBlueprint()
