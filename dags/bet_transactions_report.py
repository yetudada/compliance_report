"""
DAG to generate comprehensive reports on the bet_transactions_history table in Snowflake.

This DAG creates various reports including:
- Daily transaction summary
- Top betting patterns
- Transaction volume analysis
- Data quality checks - Hello!

All SQL queries are modularized in the include/sql folder for better maintainability.
"""

from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import dag, task
import os

# Configuration
SNOWFLAKE_CONN_ID = "snowflake"
TABLE_NAME = "demo.playtowin.bet_transactions_history"
HIGH_VALUE_THRESHOLD = 10000


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * 2",  # Every Tuesday at midnight (cron: minute hour day month weekday)
    description="Generate comprehensive reports on bet_transactions_history table",
    tags=["snowflake", "reporting", "betting", "transactions"],
    max_active_runs=1,
    default_args={
        "owner": "data_team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    template_searchpath=[f"{os.getenv('AIRFLOW_HOME')}/include/sql/bet_transactions_report"],
)
def bet_transactions_report():
    """
    Comprehensive betting transactions reporting pipeline.

    This DAG generates daily reports on betting transaction data including:
    - Transaction summaries and metrics
    - Betting pattern analysis
    - Data quality monitoring
    - High-value transaction alerts
    """

    # Task 1: Transaction Summary Report
    transaction_summary_report = SQLExecuteQueryOperator(
        task_id="generate_transaction_summary_report",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="daily_summary.sql",
        params={"table_name": TABLE_NAME},
        show_return_value_in_logs=True,
    )

    # Task 2: Top Betting Patterns Report
    betting_patterns_report = SQLExecuteQueryOperator(
        task_id="generate_betting_patterns_report",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="betting_patterns.sql",
        params={"table_name": TABLE_NAME},
        show_return_value_in_logs=True,
    )

    # Task 3: User Volume Analysis
    user_volume_report = SQLExecuteQueryOperator(
        task_id="generate_user_volume_report",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="hourly_volume.sql",
        params={"table_name": TABLE_NAME},
        show_return_value_in_logs=True,
    )

    # Task 4: Data Quality Checks
    data_quality_report = SQLExecuteQueryOperator(
        task_id="generate_data_quality_report",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="data_quality.sql",
        params={"table_name": TABLE_NAME},
        show_return_value_in_logs=True,
    )

    # Task 5: High-Value Transactions Alert
    @task
    def check_high_value_transactions():
        """
        Check for unusually high-value transactions and create alerts.
        Uses the modularized SQL file for the query.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # Read the SQL file and render with parameters
        sql_file = f"{os.getenv('AIRFLOW_HOME')}/include/sql/bet_transactions_report/high_value_transactions.sql"
        with open(sql_file, "r") as f:
            sql_template = f.read()

        # Simple parameter substitution (in production, consider using Jinja2)
        high_value_sql = sql_template.replace("{{ params.table_name }}", TABLE_NAME)
        high_value_sql = high_value_sql.replace(
            "{{ params.threshold }}", str(HIGH_VALUE_THRESHOLD)
        )

        results = hook.get_records(high_value_sql)

        if results:
            print(
                f"🚨 ALERT: Found {len(results)} high-value transactions (>${HIGH_VALUE_THRESHOLD:,})"
            )
            for row in results[:5]:  # Show top 5
                print(
                    f"  Transaction ID: {row[0]}, User: {row[1]}, Amount: ${row[2]:,.2f}"
                )
            return f"High-value transactions detected: {len(results)} found"
        else:
            print("✅ No high-value transactions detected")
            return "No high-value transactions detected"

    high_value_check = check_high_value_transactions()

    # Task 6: Generate Summary Statistics
    summary_statistics = SQLExecuteQueryOperator(
        task_id="generate_summary_statistics",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="summary_statistics.sql",
        params={"table_name": TABLE_NAME},
        show_return_value_in_logs=True,
    )

    # Define task dependencies
    [
        transaction_summary_report,
        betting_patterns_report,
        user_volume_report,
    ] >> data_quality_report
    data_quality_report >> [high_value_check, summary_statistics]


# Instantiate the DAG
bet_transactions_report_dag = bet_transactions_report()
