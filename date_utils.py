# encoding: UTF-8
#
#  Created by huawei on 2017/06/22.
#

import time
from datetime import datetime
import iso8601
from dateutil import tz
from enum import Enum

Interval = Enum('Interval', ('second', 'minute', 'm5', 'm15', 'm30', 'hour', 'day'))


def now():
    datetime.now()


def utc_now():
    datetime.utcnow()


def get_millisecond(date):
    # return int(date.strftime('%s')) * 1000
    return time.mktime(date.timetuple()) * 1000


# parse iso8601 date_str to local datetime
def parse_iso8601(date_str):
    iso_time = iso8601.parse_date(date_str)
    local_time = iso_time.astimezone(tz.tzlocal())
    return local_time


# truncate local date to interval date string
def truncate(date, interval):
    if interval == Interval.second:
        return date.strftime('%Y-%m-%d %H:%M:%S')
    elif interval == Interval.minute:
        return date.strftime('%Y-%m-%d %H:%M:00')
    elif interval == Interval.hour:
        return date.strftime('%Y-%m-%d %H:00:00')
    elif interval == Interval.day:
        utc_time_hour_0 = date.astimezone(tz.tzutc()).replace(hour=0, minute=0, second=0, microsecond=0)
        return utc_time_hour_0.astimezone(tz.tzlocal()).strftime('%Y-%m-%d %H:00:00')

    elif interval == Interval.m5:
        m5 = date.minute - date.minute % 5
        return date.replace(minute=m5).strftime('%Y-%m-%d %H:%M:00')
    elif interval == Interval.m15:
        m15 = date.minute - date.minute % 15
        return date.replace(minute=m15).strftime('%Y-%m-%d %H:%M:00')
    elif interval == Interval.m30:
        m30 = date.minute - date.minute % 30
        return date.replace(minute=m30).strftime('%Y-%m-%d %H:%M:00')


def test_truncate():
    date_strs = ['2017-06-27T09:02:25.013Z', '2017-06-27T09:06:25.013Z', '2017-06-27T09:12:25.013Z',
                 '2017-06-26T09:22:22.013Z', '2017-06-27T09:32:25.013Z', '2017-06-27T09:42:25.013Z',
                 '2017-06-27T09:46:25.013Z']

    for date_str in date_strs:
        local_time = parse_iso8601(date_str)
        print 'parsing time {}'.format(local_time)
        print truncate(local_time, Interval.second)
        print truncate(local_time, Interval.minute)
        print truncate(local_time, Interval.hour)
        print truncate(local_time, Interval.day)
        print truncate(local_time, Interval.m5)
        print truncate(local_time, Interval.m15)
        print truncate(local_time, Interval.m30)
        print '\n'



def test():
    # calculate delta
    # iso_time2 = iso8601.parse_date('2017-06-27T09:47:22.013Z')
    # print(get_millisecond(iso_time) - get_millisecond(iso_time2))

    # parse iso date
    date_str = '2017-06-27T09:47:25.013Z'
    iso_time = parse_iso8601(date_str)
    print iso_time
    print iso_time.strftime('%Y-%m-%d %H:%M:%S')
    # print iso_time.astimezone(tz.tzlocal)

    # convert time zone
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    # utc = datetime.utcnow()
    # utc = datetime.strptime('2017-06-21 02:37:21', '%Y-%m-%d %H:%M:%S')

    # Tell the datetime object that it's in UTC time zone since
    # datetime objects are 'naive' by default
    # utc = iso_time.replace(tzinfo=from_zone)
    # print utc
    # print utc.strftime('%Y-%m-%d %H:%M:%S')

    # Convert time zone
    central = iso_time.astimezone(to_zone)
    print central
    print central.strftime('%Y-%m-%d %H:%M:%S')

    # test iso date
    print parse_iso8601('2017-06-27T09:47:25.013Z').strftime('%Y-%m-%d %H:%M:%S')


# test_truncate()
