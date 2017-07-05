# encoding: UTF-8
#
#  Created by huawei on 2017/06/22.
#

from db.database import Connection
import settings
import constants

db = Connection(
    host=settings.MYSQL_HOST,
    database=settings.MYSQL_DATABASE,
    user=settings.MYSQL_USER,
    password=settings.MYSQL_PASSWORD)


# get instrument by symbol
def get_instrument(symbol):
    instrument = db.get('select * from instrument where symbol=%s', symbol)
    return instrument


# select instrument history
def get_history(repo, instrument_id, create_date):
    history = db.get('select * from {} where instrument_id = %s and create_date = %s'.format(repo),
                     instrument_id, create_date)
    return history


# update single history table
def update_history(repo, instrument_id, create_date, quote):
    db.execute("""update {} set open_price = %s, close_price = %s, high_price = %s, low_price = %s, volume = %s, bid= %s, ask= %s
                where instrument_id = %s and create_date = %s""".format(repo),
               quote[constants.OPEN_PRICE], quote[constants.CLOSE_PRICE], quote[constants.HIGH_PRICE], quote[constants.LOW_PRICE],
               quote[constants.VOLUME], quote[constants.BID], quote[constants.ASK],
               instrument_id, create_date)


# insert new row into single history table
def insert_history(repo, instrument_id, create_date, quote):
    db.execute(
        """insert into {} (create_date, instrument_id, volume, open_price, close_price, high_price, low_price, `type`, bid, ask)
         values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""".format(repo),
        create_date, instrument_id, quote[constants.VOLUME], quote[constants.OPEN_PRICE], quote[constants.CLOSE_PRICE],
        quote[constants.HIGH_PRICE], quote[constants.LOW_PRICE], quote[constants.TYPE], quote[constants.BID],
        quote[constants.ASK]
    )


def commit():
    db.commit()


# def main():
#     instrument = get_instrument('ag1712')
#     print instrument

    # // update history
    # quote = {constants.OPEN_PRICE: 1, constants.CLOSE_PRICE: 2,  constants.HIGH_PRICE: 3,
    #          constants.LOW_PRICE: 4, constants.TYPE: 'future', constants.VOLUME: 5, constants.ASK: 6, constants.BID: 7}
    # update_history('instrument_history_m', 32, '2017-06-27 09:51:31+0000', quote)
    #
    # history = get_history('instrument_history_m', 32, '2017-06-27 09:51:31+0000')
    # print history

    #  // insert into history
    # quote = {constants.OPEN_PRICE: 92, constants.CLOSE_PRICE: 123, constants.HIGH_PRICE: 132,
    #          constants.LOW_PRICE: 92, constants.TYPE: 'future', constants.VOLUME: 1, constants.ASK: 1,
    #          constants.BID: 1}
    # insert_history('instrument_history_m', 32, '2017-06-27 09:51:34', quote)
    # history = get_history('instrument_history_m', 32, '2017-06-27 09:51:34')
    # print history

#
# if __name__ == '__main__':
#     main()
