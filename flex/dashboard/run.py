from datetime import datetime, timedelta

import mysql_to_bigquery

now = datetime.now()
date_end = now - timedelta(days=now.weekday() + 1)
date_start = date_end - timedelta(days=6)
date_start = date_start.strftime("%Y-%m-%d")
date_end = date_end.strftime("%Y-%m-%d")
print(date_start, date_end)
app_id = 'orange_dashboard'
report_type = 'bigquery_flex_events_daydiff_2021'
bq_dataset = ''
bq_table = ''

# TODO functions launcher orchestrator

mysql_to_bigquery.report(date_start, date_end, app_id, report_type, bq_dataset, bq_table)
