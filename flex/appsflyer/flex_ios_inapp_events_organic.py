import os
from datetime import date

import mysql.connector

import functions


def report(date_target_start: date, date_target_end: date):
    date_start = date_target_start
    date_end = date_target_end
    app_id = 'id1441116618'
    report_type = 'organic_in_app_events_report'
    sfx = '&timezone=Europe%2fWarsaw&additional_fields=device_model,keyword_id,store_reinstall,deeplink_url,oaid,amazon_aid,keyword_match_type,att,conversion_type,campaign_type&maximum_rows=1000000'

    credentials = functions.get_token(app_id, report_type)

    params = {
        'api_token': credentials['api_token'],
        'from': date_start,
        'to': date_end,
        'sfx': sfx
    }

    url = 'https://hq.appsflyer.com/export/{app}/{report}/v5?api_token={token}&from={date_from}&to={date_to}{metrics}'.format(
        app=app_id,
        report=report_type,
        token=params['api_token'],
        date_from=date_start,
        date_to=date_end,
        metrics=params['sfx']
    )

    def insert_row(cursor, row):
        try:
            cursor.execute("""
                INSERT INTO Orange_Flex_Organic_In_App_Events(
                    Attributed_Touch_Type,
                    Attributed_Touch_Time,
                    Install_Time,
                    Event_Time,
                    Event_Name,
                    Event_Value,
                    Event_Revenue,
                    Event_Revenue_Currency,
                    Event_Revenue_USD,
                    Event_Source,
                    Is_Receipt_Validated,
                    Partner,
                    Media_Source,
                    Channel,
                    Keywords,
                    Campaign,
                    Campaign_ID,
                    Adset,
                    Adset_ID,
                    Ad,
                    Ad_ID,
                    Ad_Type,
                    Site_ID,
                    Sub_Site_ID,
                    Sub_Param_1,
                    Sub_Param_2,
                    Sub_Param_3,
                    Sub_Param_4,
                    Sub_Param_5,
                    Cost_Model,
                    Cost_Value,
                    Cost_Currency,
                    Contributor_1_Partner,
                    Contributor_1_Media_Source,
                    Contributor_1_Campaign,
                    Contributor_1_Touch_Type,
                    Contributor_1_Touch_Time,
                    Contributor_2_Partner,
                    Contributor_2_Media_Source,
                    Contributor_2_Campaign,
                    Contributor_2_Touch_Type,
                    Contributor_2_Touch_Time,
                    Contributor_3_Partner,
                    Contributor_3_Media_Source,
                    Contributor_3_Campaign,
                    Contributor_3_Touch_Type,
                    Contributor_3_Touch_Time,
                    Region,
                    Country_Code,
                    State,
                    City,
                    Postal_Code,
                    DMA,
                    IP,
                    WIFI,
                    Operator,
                    Carrier,
                    Language,
                    AppsFlyer_ID,
                    Advertising_ID,
                    IDFA,
                    Android_ID,
                    Customer_User_ID,
                    IMEI,
                    IDFV,
                    Platform,
                    Device_Type,
                    OS_Version,
                    App_Version,
                    SDK_Version,
                    App_ID,
                    App_Name,
                    Bundle_ID,
                    Is_Retargeting,
                    Retargeting_Conversion_Type,
                    Attribution_Lookback,
                    Reengagement_Window,
                    Is_Primary_Attribution,
                    User_Agent,
                    HTTP_Referrer,
                    Original_URL,
                    Device_Model,
                    Keyword_ID,
                    Store_Reinstall,
                    Deeplink_URL,
                    OAID,
                    Amazon_Fire_ID,
                    Keyword_Match_Type,
                    ATT,
                    Conversion_Type,
                    Campaign_Type
                )
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )""",
                           (
                               row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                               row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20],
                               row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29], row[30],
                               row[31], row[32], row[33], row[34], row[35], row[36], row[37], row[38], row[39], row[40],
                               row[41], row[42], row[43], row[44], row[45], row[46], row[47], row[48], row[49], row[50],
                               row[51], row[52], row[53], row[54], row[55], row[56], row[57], row[58], row[59], row[60],
                               row[61], row[62], row[63], row[64], row[65], row[66], row[67], row[68], row[69], row[70],
                               row[71], row[72], row[73], row[74], row[75], row[76], row[77], row[78], row[79], row[80],
                               row[81], row[82], row[83], row[84], row[85], row[86], row[87], row[88], row[89], row[90]
                           )
                           )
        except mysql.connector.Error as e:
            functions.error_log(app_id, report_type, e.args[0], e.args[1])
            return

    script_name = os.path.basename(__file__)
    functions.get_data(url=url, params=params, app_id=app_id, report_type=report_type, insert_function=insert_row,
                       date_start=date_start, date_end=date_end, script_name=script_name)
