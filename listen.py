# encoding: UTF-8

#!/usr/bin/env python
import pika
import sys
import avro.schema
import avro.io
import json
from datetime import datetime
from StringIO import StringIO
import threading
import time
from algoLogger import AlgoLogger
import settings
import collector


logger = AlgoLogger.get_logger(__name__)

#global variables
schema_dict = {}


def callback(ch, method, properties, body):
    global schema_dict
    # process as avro encoded body, using the last part of the
    # routing_key as the avro message type
    schema_type = method.routing_key.rsplit('.',1)[-1]
    if schema_type in schema_dict:
        schema = schema_dict[schema_type]
    else:
        schema = avro.schema.parse(open(settings.AVRO_MESSAGES_PATH+'/'+schema_type+'.avro').read())
        schema_dict[schema_type] = schema
    buffer_reader = StringIO(body)
    buffer_decoder = avro.io.BinaryDecoder(buffer_reader)
    datum_reader = avro.io.DatumReader(schema)
    msg = datum_reader.read(buffer_decoder)
    # distribute message
    distribute_message(schema_type, msg)
    writelog(logger, method.routing_key, msg)


def distribute_message(schema_type, msg_body):
    # logger.debug(" ====>   get symbol message {}  =====".format(msg_body["symbol"]))
    collector.subscribe(msg_body)

def writelog(logger, routing_key, body):
    data = {
        'routing_key': routing_key,
        'record_time': str(datetime.now()),
        'body': body,
    }
    # logger.debug(json.dumps(data))


def setup_listener():

    logger.debug("setting up avro message consumer")
    exchange_arg = settings.RabbitMQ_EXCHANGE
    binding_keys = ["com.#.InstrumentMessage"]

    try:
        parameters = pika.URLParameters(settings.RabbitMQ_URL)
        connection = pika.BlockingConnection(parameters)

    except:
        sys.stderr.write("%s - cannot connect to RabbitMQ\n" % sys.argv[0])
        sys.exit(1)

    channel = connection.channel()

    channel.exchange_declare(exchange=exchange_arg,
                             type='topic')

    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    for binding_key in binding_keys:
        channel.queue_bind(exchange=exchange_arg,
                           queue=queue_name,
                           routing_key=binding_key)

    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()


def startup():
    threading.Thread(target=setup_listener, args=(), name='thread-avro-listener').start()
    time.sleep(2)
    logger.debug("Finished to set up avro message listener")

def main():
    setup_listener()

# Run the tests if the file is called directly.
# if __name__ == '__main__':
#     main()


# Run the tests if the file is called directly.
if __name__ == '__main__':
    main()
