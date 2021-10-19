from datetime import datetime, timedelta

import mysql_to_bigquery

now = datetime.now()
date_end = now - timedelta(days=now.weekday() + 1)
# date_start = date_end - timedelta(days=6)
# date_start = date_start.strftime("%Y-%m-%d")
date_start = "2021-01-01"
date_end = date_end.strftime("%Y-%m-%d")
date_end = '2021-01-31'
print(date_start, date_end)

# TODO functions launcher orchestrator
app_id = 'orange_dashboard'
bq_dataset = 'orange'

# bq_table = 'Flex_Installs'
# report_type = 'bigquery_flex_installs_2021'
# mysql_to_bigquery.report(date_start, date_end, app_id, report_type, bq_dataset, bq_table)

# bq_table = 'Events_Daysdiff_by_City'
# report_type = 'bigquery_flex_events_2021'
# mysql_to_bigquery.report(date_start, date_end, app_id, report_type, bq_dataset, bq_table)

bq_table = 'Flex_Events'
report_type = 'bigquery_flex_events_daydiff_2021'
mysql_to_bigquery.report(date_start, date_end, app_id, report_type, bq_dataset, bq_table)