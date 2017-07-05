# encoding: UTF-8
#
#  Created by huawei on 2017/06/22.
#


from algoLogger import AlgoLogger
from db import db_service
import constants
import date_utils
from date_utils import Interval

# instrumentMessage trigger,  get symbol, then get instrument object

# get instrument history by id and date

logger = AlgoLogger.get_logger(__name__)

# cache to store symbol:instrument_object
instrument_cache = {}


# triggered in listen
def subscribe(msg):
    symbol = msg[constants.SYMBOL]

    # if symbol != 'ag1712':
    #     return

    logger.debug('Got new InstrumentMessage {} on {} : {}'.format(symbol, msg[constants.CREATE_DATE], ''))
    # update instrument
    instrument = get_instrument(symbol)
    instrument[constants.PRICE] = msg[constants.PRICE]
    instrument[constants.BID] = msg[constants.BID]
    instrument[constants.ASK] = msg[constants.ASK]
    instrument[constants.AMOUNT] = msg[constants.AMOUNT]

    # TODO , copy whole msg for back up

    # update msg
    msg[constants.TYPE] = instrument[constants.TYPE]
    msg[constants.INSTRUMENT_ID] = instrument[constants.INSTRUMENT_ID]

    # parse iso date string to local date
    local_date = date_utils.parse_iso8601(msg[constants.CREATE_DATE])

    logger.debug('Start ===>  updating {} on {}'.format(symbol, msg[constants.CREATE_DATE]))
    update_history(msg, local_date)
    logger.debug('End <==== updating {} on {}'.format(symbol, msg[constants.CREATE_DATE]))


def get_instrument(symbol):
    if instrument_cache.has_key(symbol):
        return instrument_cache[symbol]
    else:
        instrument = db_service.get_instrument(symbol)
        instrument_cache[symbol] = instrument
        return instrument


# update instrument history
def update_history(instrument, date):

    #update second / minute table
    update_history_db(Interval.second, constants.REPO_SECOND, instrument, date)
    update_history_db(Interval.minute, constants.REPO_MINUTE, instrument, date)

    # update candlestick table
    update_candlestick_history_db(Interval.hour, constants.REPO_HOUR, instrument, date)
    update_candlestick_history_db(Interval.day, constants.REPO_DAY, instrument, date)
    update_candlestick_history_db(Interval.m5, constants.REPO_M5, instrument, date)
    update_candlestick_history_db(Interval.m15, constants.REPO_M15, instrument, date)
    update_candlestick_history_db(Interval.m30, constants.REPO_M30, instrument, date)

    # commit after all updating
    db_service.commit()


# update instrument history table:  history_s / history_m, without calculate high / low price (IOW, candle stick)
def update_history_db(interval, repo, instrument, local_date):
    # truncate time
    date = date_utils.truncate(local_date, interval)
    instrument_id = instrument[constants.INSTRUMENT_ID]
    symbol = instrument[constants.SYMBOL]

    price = instrument[constants.PRICE]
    instrument[constants.OPEN_PRICE] = price
    instrument[constants.CLOSE_PRICE] = price
    instrument[constants.HIGH_PRICE] = price
    instrument[constants.LOW_PRICE] = price

    record = db_service.get_history(repo, instrument_id, date)
    if record is None:
        db_service.insert_history(repo, instrument_id, date, instrument)
        # logger.debug('===> Insert new row for {} on {}'.format(symbol, date))
    else:
        db_service.update_history(repo, instrument_id, date, instrument)
        # logger.debug('  <=====>  Update row for {} on {}'.format(symbol, date))


# update candlestick db table
def update_candlestick_history_db(interval, repo, instrument, local_date):
    # truncate time
    date = date_utils.truncate(local_date, interval)
    instrument_id = instrument[constants.INSTRUMENT_ID]
    symbol = instrument[constants.SYMBOL]

    price = instrument[constants.PRICE]

    # get latest record
    record = db_service.get_history(repo, instrument_id,  date)
    if record is None:
        instrument[constants.OPEN_PRICE] = price
        instrument[constants.CLOSE_PRICE] = price
        instrument[constants.HIGH_PRICE] = price
        instrument[constants.LOW_PRICE] = price

        # insert new row
        db_service.insert_history(repo, instrument_id, date, instrument)
        # logger.debug('===> Insert new row for {} on {}'.format(symbol, date))
    else:

        # calculate high / low price
        high_price = max(price, record[constants.HIGH_PRICE])
        low_price = min(price, record[constants.LOW_PRICE])

        instrument[constants.OPEN_PRICE] = record[constants.OPEN_PRICE]
        instrument[constants.CLOSE_PRICE] = price
        instrument[constants.HIGH_PRICE] = high_price
        instrument[constants.LOW_PRICE] = low_price

        # update row
        db_service.update_history(repo, instrument_id, date, instrument)
        # logger.debug('  <=====>  Update row for {} on {}'.format(symbol, date))


