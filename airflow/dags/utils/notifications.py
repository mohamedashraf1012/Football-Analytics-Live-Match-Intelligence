import os
import logging
from datetime import datetime

log = logging.getLogger(__name__)

TEAM_EMAIL = os.getenv("AIRFLOW__SMTP__SMTP_MAIL_FROM", "")


def on_failure_callback(context: dict):
    """Airflow failure callback — logs the error and sends an email if SMTP is configured."""
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exec_dt = context["execution_date"]
    log_url = context["task_instance"].log_url

    log.error(
        f"\nTASK FAILED\n"
        f"  DAG:   {dag_id}\n"
        f"  Task:  {task_id}\n"
        f"  Time:  {exec_dt}\n"
        f"  Logs:  {log_url}\n"
    )

    if TEAM_EMAIL:
        try:
            from airflow.utils.email import send_email
            send_email(
                to           = [TEAM_EMAIL],
                subject      = f"[FootballFlow] Pipeline failed — {task_id}",
                html_content = f"""
                <h2>FootballFlow — Task Failed</h2>
                <table border="1" cellpadding="8" style="border-collapse:collapse">
                  <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
                  <tr><td><b>Task</b></td><td>{task_id}</td></tr>
                  <tr><td><b>Run Time</b></td><td>{exec_dt}</td></tr>
                  <tr><td><b>Logs</b></td><td><a href="{log_url}">{log_url}</a></td></tr>
                </table>
                """,
            )
        except Exception as e:
            log.warning(f"Could not send failure email: {e}")


def notify_pipeline_success(**context):
    """Final task callback — logs success summary and sends email if SMTP is configured."""
    dag_id  = context["dag"].dag_id
    exec_dt = context["execution_date"]
    now     = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    log.info(
        f"\nFOOTBALLFLOW PIPELINE COMPLETE\n"
        f"  DAG:      {dag_id}\n"
        f"  Run:      {exec_dt}\n"
        f"  Finished: {now}\n"
    )

    if TEAM_EMAIL:
        try:
            from airflow.utils.email import send_email
            send_email(
                to           = [TEAM_EMAIL],
                subject      = "[FootballFlow] Batch pipeline completed",
                html_content = f"""
                <h2 style="color:green">FootballFlow — Pipeline Complete</h2>
                <table border="1" cellpadding="8" style="border-collapse:collapse">
                  <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
                  <tr><td><b>Run Time</b></td><td>{exec_dt}</td></tr>
                  <tr><td><b>Finished</b></td><td>{now}</td></tr>
                </table>
                <p>Power BI dashboards are ready for refresh.</p>
                """,
            )
        except Exception as e:
            log.warning(f"Could not send success email: {e}")
