from datetime import date, timedelta
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