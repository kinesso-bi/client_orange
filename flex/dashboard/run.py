from datetime import datetime, timedelta

import mysql_to_bigquery

now = datetime.now()
date_end = now - timedelta(days=1)
date_start = date_end.strftime("%Y-%m-%d")
date_end = date_start
print(date_start, date_end)

# TODO functions launcher orchestrator
app_id = 'orange_dashboard'
bq_dataset = 'orange'

bq_table = 'Flex_Events'
report_type = 'bigquery_flex_events_daydiff_2021'
mysql_to_bigquery.report(date_start, date_end, app_id, report_type, bq_dataset, bq_table)

bq_table = 'Flex_Installs'
report_type = 'bigquery_flex_installs_2021'
mysql_to_bigquery.report(date_start, date_end, app_id, report_type, bq_dataset, bq_table)