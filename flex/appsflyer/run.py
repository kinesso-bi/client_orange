from datetime import date, timedelta

import flex_android_installs
import flex_android_uninstalls
import flex_android_inapp_events
import flex_android_conversion_retargeting
import flex_android_installs_organic
import flex_android_inapp_events_organic
import flex_android_inapp_events_retargeting
import flex_ios_installs
import flex_ios_uninstalls
import flex_ios_inapp_events
import flex_ios_conversion_retargeting
import flex_ios_installs_organic
import flex_ios_inapp_events_organic
import flex_ios_inapp_events_retargeting

beg = 49
start = date.today() - timedelta(days=49)
end = date.today() - timedelta(days=beg-2)
print(start, end)
# todo logs.html file


# flex_ios_uninstalls.report(date_target_start=start,date_target_end=end)
# print(1)
# flex_android_uninstalls.report(date_target_start=start,date_target_end=end)
# print(2)
# flex_ios_installs.report(date_target_start=start,date_target_end=end)
# print(3)
# flex_android_installs.report(date_target_start=start,date_target_end=end)
# print(4)
# flex_android_inapp_events.report(date_target_start=start,date_target_end=end)
# print(5)
# flex_android_inapp_events_retargeting.report(date_target_start=start,date_target_end=end)

