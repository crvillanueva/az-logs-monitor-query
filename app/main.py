import json
import sys
from datetime import datetime, timedelta

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.monitor.query import (
    LogsQueryClient,
    LogsQueryResult,
    LogsTableRow,
    MetricsQueryClient,
)


def main(
    workspace_id: str, app_name: str, n_logs: str, n_days_before: int = None
) -> None:
    if n_days_before is None:
        n_days_before = 1
    # allow only cli auth
    credential = DefaultAzureCredential(
        exclude_cli_credential=False,
        exclude_environment_credential=True,
        exclude_powershell_credential=True,
        exclude_shared_token_cache_credential=True,
        exclude_interactive_browser_credential=True,
        exclude_visual_studio_code_credential=True,
    )

    logs_client = LogsQueryClient(credential)
    metrics_client = MetricsQueryClient(credential)

    query_results: LogsQueryResult = logs_client.query_workspace(
        workspace_id=workspace_id,
        query=f"""AppRequests | where AppRoleName == '{app_name}' | take {n_logs}""",
        timespan=((datetime.now() - timedelta(days=n_days_before)), datetime.now()),
    )

    rows: LogsTableRow = query_results.tables[0].rows
    columns = query_results.tables[0].columns

    rows_as_dict = []
    for row in rows:
        # each row is a list of properties
        row_dict = {}
        for column, value in zip(columns, row):
            if column == "Properties":
                value = json.loads(value)
            row_dict[column] = value
        rows_as_dict.append(row_dict)

    df = pd.json_normalize(rows_as_dict)
    df = df[
        [
            "Name",
            "AppRoleName",
            "Success",
            "DurationMs",
            # "AppRoleInstance",
            "Properties.TriggerReason",
        ]
    ]
    df["DurationMs"] = df["DurationMs"].astype(float)
    print(df)


if __name__ == "__main__":
    args = sys.argv
    days: str | None
    try:
        days = int(args[3])
    except IndexError:
        days = 5
    print(f"Request last '{args[1]}' execution logs for workspace {args[3]}")
    main(workspace_id=args[1], app_name=args[2], n_logs=args[3], n_days_before=days)
