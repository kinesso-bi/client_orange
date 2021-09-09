from csv import reader
from datetime import date

import mysql.connector

import functions


def report(date_target_start: date, date_target_end: date):
    date_start = date_target_start
    date_end = date_target_end
    app_id = 'id1441116618'
    report_type = 'installs_report'
    sfx = '&timezone=Europe%2fWarsaw&additional_fields=install_app_store,contributor1_match_type,contributor2_match_type,contributor3_match_type,match_type,device_category,gp_referrer,gp_click_time,gp_install_begin,amazon_aid,keyword_match_type&reattr=true&maximum_rows=1000000'

    credentials = functions.get_token(app_id, report_type)

    params = {
        'api_token': credentials['api_token'],
        'from': date_start,
        'to': date_end,
        'sfx': sfx
    }

    url = 'https://hq.appsflyer.com/export/{app}/{report}/v5?api_token={token}{metrics}'.format(app=app_id,
                                                                                                report=report_type,
                                                                                                token=params[
                                                                                                    'api_token'],
                                                                                                metrics=params['sfx'])

    def insert_row(cursor, row):
        try:
            cursor.execute("""
                       INSERT INTO flex_retargeting_conversions(
                            AttributedTouchType,
                            AttributedTouchTime,
                            InstallTime,
                            EventTime,
                            EventName,
                            EventValue,
                            EventRevenue,
                            EventRevenueCurrency,
                            EventRevenueUSD,
                            EventSource,
                            IsReceiptValidated,
                            Partner,
                            MediaSource,
                            Channel,
                            Keywords,
                            Campaign,
                            CampaignID,
                            Adset,
                            AdsetID,
                            Ad,
                            AdID,
                            AdType,
                            SiteID,
                            SubSiteID,
                            SubParam1,
                            SubParam2,
                            SubParam3,
                            SubParam4,
                            SubParam5,
                            CostModel,
                            CostValue,
                            CostCurrency,
                            Contributor1Partner,
                            Contributor1MediaSource,
                            Contributor1Campaign,
                            Contributor1TouchType,
                            Contributor1TouchTime,
                            Contributor2Partner,
                            Contributor2MediaSource,
                            Contributor2Campaign,
                            Contributor2TouchType,
                            Contributor2TouchTime,
                            Contributor3Partner,
                            Contributor3MediaSource,
                            Contributor3Campaign,
                            Contributor3TouchType,
                            Contributor3TouchTime,
                            Region,
                            CountryCode,
                            State,
                            City,
                            PostalCode,
                            DMA,
                            IP,
                            WIFI,
                            Operator,
                            Carrier,
                            Language,
                            AppsFlyerID,
                            AdvertisingID,
                            IDFA,
                            AndroidID,
                            CustomerUserID,
                            IMEI,
                            IDFV,
                            Platform,
                            DeviceType,
                            OSVersion,
                            AppVersion,
                            SDKVersion,
                            AppID,
                            AppName,
                            BundleID,
                            IsRetargeting,
                            RetargetingConversionType,
                            AttributionLookback,
                            ReengagementWindow,
                            IsPrimaryAttribution,
                            UserAgent,
                            HTTPReferrer,
                            OriginalURL,
                            InstallAppStore,
                            Contributor1MatchType,
                            Contributor2MatchType,
                            Contributor3MatchType,
                            MatchType,
                            DeviceCategory,
                            GooglePlayReferrer,
                            GooglePlayClickTime,
                            GooglePlayInstallBeginTime,
                            AmazonFireID,
                            KeywordMatchType
                       )
                       VALUES(
                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                       %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                       %s,%s)""",
                           (
                               row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10],
                               row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20],
                               row[21], row[22], row[23], row[24], row[25], row[26], row[27], row[28], row[29], row[30],
                               row[31], row[32], row[33], row[34], row[35], row[36], row[37], row[38], row[39], row[40],
                               row[41], row[42], row[43], row[44], row[45], row[46], row[47], row[48], row[49], row[50],
                               row[51], row[52], row[53], row[54], row[55], row[56], row[57], row[58], row[59], row[60],
                               row[61], row[62], row[63], row[64], row[65], row[66], row[67], row[68], row[69], row[70],
                               row[71], row[72], row[73], row[74], row[75], row[76], row[77], row[78], row[79], row[80],
                               row[81], row[82], row[83], row[84], row[85], row[86], row[87], row[88], row[89], row[90],
                               row[91]
                           )
                           )
        except mysql.connector.Error as e:
            functions.error_log(app_id, report_type, e.args[0], e.args[1])
            pass

    data = functions.get_stream(get_url=url, get_params=params, get_app_id=app_id, get_report_type=report_type)
    if data is None:
        return
    else:
        cnx = functions.db_connect(app_id, report_type)
        cursor = functions.get_cursor(app_id, report_type, cnx)
        if cursor is None:
            return
        else:
            for line in reader(data):
                insert_row(cursor=cursor, row=line)
            functions.db_disconnect(app_id, report_type, cnx, cursor, date_target_start=date_start,
                                    date_target_end=date_end, db_target="flex_ios_conversion_retargeting.py")
