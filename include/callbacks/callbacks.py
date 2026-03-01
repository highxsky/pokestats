# Imports
import logging

# Logging
LOG = logging.getLogger(__name__)

# Function
def notify_on_failure(context):
    from airflow.models import Variable
    from airflow.utils.email import send_email

    alert_email = Variable.get("alert_email", default_var=None)
    if not alert_email:
        LOG.warning("Alert email variable not set, skipping failure email.")
        return None

    dag_id  = context["dag"].dag_id
    run_id  = context["dag_run"].run_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    exc     = context.get("exception")

    send_email(
        to=[alert_email],
        subject=f"[Airflow] DAG `{dag_id}` failed",
        html_content=f"""
            <h3>Pipeline Failure Alert</h3>
            <b>DAG:</b> {dag_id}<br>
            <b>Run ID:</b> {run_id}<br>
            <b>Failed Task:</b> {task_id}<br>
            <b>Error:</b> {exc}<br>
            <b>Logs:</b> <a href="{log_url}">View logs</a>
        """,
    )
