from datetime import date, timedelta
import functions
import flex_android_conversion_retargeting
import flex_android_inapp_events
import flex_android_inapp_events_organic
import flex_android_inapp_events_retargeting
import flex_android_installs
import flex_android_installs_organic
import flex_android_uninstalls
import flex_ios_conversion_retargeting
import flex_ios_inapp_events
import flex_ios_inapp_events_organic
import flex_ios_inapp_events_retargeting
import flex_ios_installs
import flex_ios_installs_organic
import flex_ios_uninstalls

yesterday = date.today() - timedelta(days=1)
print(yesterday)

functions.error_log("1", "2", "3", "hello")

# flex_android_conversion_retargeting.report(date_target_start=yesterday, date_target_end=yesterday)
# flex_android_installs.report(date_target_start=yesterday, date_target_end=yesterday)
# flex_android_installs_organic.report(date_target_start=yesterday, date_target_end=yesterday)
# flex_android_uninstalls.report(date_target_start=yesterday, date_target_end=yesterday)
# flex_ios_conversion_retargeting.report(date_target_start=yesterday, date_target_end=yesterday)
# flex_ios_installs.report(date_target_start=yesterday, date_target_end=yesterday)
# flex_ios_installs_organic.report(date_target_start=yesterday, date_target_end=yesterday)
# flex_ios_uninstalls.report(date_target_start=yesterday, date_target_end=yesterday)
# """ bigger files, run at the end """
# flex_android_inapp_events_retargeting.report(date_target_start=yesterday, date_target_end=yesterday)
# flex_ios_inapp_events_retargeting.report(date_target_start=yesterday, date_target_end=yesterday)
# flex_android_inapp_events_organic.report(date_target_start=yesterday, date_target_end=yesterday)
# flex_ios_inapp_events_organic.report(date_target_start=yesterday, date_target_end=yesterday)
# flex_android_inapp_events.report(date_target_start=yesterday, date_target_end=yesterday)
# flex_ios_inapp_events.report(date_target_start=yesterday, date_target_end=yesterday)


# # TODO run below for ios
# targets = ['2021-09-10','2021-09-13','2021-09-16','2021-09-19','2021-09-22']
# target_ends = ['2021-09-12','2021-09-15','2021-09-18','2021-09-21','2021-09-22']
# for target, target_end in zip(targets, target_ends):
#     print(target, target_end)
    # flex_android_inapp_events.report(date_target_start=target, date_target_end=target_end)
    # flex_ios_inapp_events.report(date_target_start=target, date_target_end=target_end)


# from datetime import datetime
# for i in range(3,0,-1):
#     print(i)
#     yesterday = (datetime.strptime('2021-09-20',"%Y-%m-%d") - timedelta(days=i)).strftime("%Y-%m-%d")
#     print(yesterday)
#     flex_android_inapp_events_organic.report(date_target_start=yesterday, date_target_end=yesterday)
#     flex_ios_inapp_events_organic.report(date_target_start=yesterday, date_target_end=yesterday)