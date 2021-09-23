import os
from datetime import date

import functions


def report(date_target_start: date, date_target_end: date, mysql_app_id, mysql_table, bq_dataset, bq_table):
    date_start = date_target_start
    date_end = date_target_end
    app_id = mysql_app_id
    source_table = mysql_table
    target_dataset = bq_dataset
    target_table = bq_table

    def select_query():
        try:
            query = "SELECT * FROM {} WHERE `Date` BETWEEN CAST('{}' AS DATE) AND CAST('{}' AS DATE);".format(
                source_table,
                date_start,
                date_end)
            # query = "SELECT Distinct `Date` FROM bigquery_flex_events_daydiff_2021 ORDER BY `Date` DESC LIMIT 90;"
            functions.success_log(app_id, source_table, 1, "Query started")
            return query
        except Exception as e:
            functions.error_log(app_id, source_table, -1, e)
            return None

    script_name = os.path.basename(__file__)
    functions.migrate_data(app_id=app_id, report_type=source_table, select_function=select_query,
                           dataset_target=target_dataset, table_target=target_table,
                           date_start=date_start, date_end=date_end, script_name=script_name)
