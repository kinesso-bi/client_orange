import json
from csv import reader
from datetime import date, timedelta

import mysql.connector
import requests

with open('credentials.json') as file:
    credentials = json.load(file)

app_id = 'com.orange.rn.dop'
report_type = 'installs_report'
yesterday = date.today() - timedelta(days=1)

params = {
    'api_token': credentials['api_token'],
    'from': yesterday,
    'to': yesterday,
    'sfx': ''
}

url = 'https://hq.appsflyer.com/export/{}/{}/v5?api_token={}{}'.format(app_id, report_type, params['api_token'],
                                                                       params['sfx'])


def get_stream(get_url, get_params):
    response = requests.request('GET', url=get_url, params=get_params)
    if response.status_code != 200:
        if response.status_code == 400:
            # TODO error log
            print("")
        else:
            # TODO error log
            print("")
    else:
        stream = response.text
        return stream.split('\n')[1:-1]


def insert_row(cursor, row):
    try:
        cursor.execute("""
                   INSERT INTO flex_installs(
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
                   %s,%s
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
                           row[81], row[82], row[83], row[84], row[85], row[86], row[87], row[88], row[89], row[90],
                           row[91]
                       )
                       )
    except mysql.connector.Error as err:
        # TODO error log
        print(err)
        pass


data = get_stream(get_url=url, get_params=params)

cnx = mysql.connector.connect(
    user=credentials['user'],
    password=credentials['password'],
    host=credentials['host'],
    database=credentials['database'])

cursor = cnx.cursor()

for line in reader(data):
    insert_row(cursor=cursor, row=line)

cnx.commit(),
cursor.close()
cnx.close()
