#!/usr/bin/env python
import pika
import sys
import signal
import sys
import argparse
import avro.schema
import avro.io
import json
from datetime import datetime
from StringIO import StringIO

#global variables
args = None
schema_dict = {}

def signal_handler(signal, frame):
        sys.exit(0)

def process_log(logfile, channel, exchange):
    global args
    global schema_dict
    with open(logfile) as f:
        for line in f:
            data = json.loads(line)
            timestamp = data['record_time']
            routing_key = data['routing_key']
            body = data['body']
            if (args.avro_schema_dir != ''):
                schema_type = routing_key.rsplit('.',1)[-1]
                if schema_type in schema_dict:
                    schema = schema_dict[schema_type]
                else:
                    schema = avro.schema.parse(open(args.avro_schema_dir+'/'+schema_type+'.avro').read())
                buffer_writer = StringIO()
                buffer_encoder = avro.io.BinaryEncoder(buffer_writer)
                datum_writer = avro.io.DatumWriter(schema)
                datum = body
                datum_writer.write(datum, buffer_encoder)
                msg=buffer_writer.getvalue()
            else:
                msg=str(body)

            channel.basic_publish(exchange=exchange,
                      routing_key=routing_key,
                      body=msg)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    parser = argparse.ArgumentParser()
    parser.add_argument("--url",
        default = 'amqp://guest:guest@localhost:5672/%2F',
        help="connection url like amqp://username:password@host:port/<virtual_host>[?query-string]")
    parser.add_argument("-e", "--exchange",
        default='',
        help="exchange name")
    parser.add_argument("--avro_schema_dir",
        default='',
        help="exchange name")
    parser.add_argument("logfile",
        help="input log file")
    global args
    args = parser.parse_args()
    exchange = args.exchange

    try:
        parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')
        connection = pika.BlockingConnection(parameters)

    except:
        sys.stderr.write("%s - cannot connect to RabbitMQ\n" % sys.argv[0])
        sys.exit(1)

    channel = connection.channel()

    channel.exchange_declare(exchange=exchange,
                             type='topic')
    process_log(args.logfile, channel, exchange)

    connection.close()

# Run the tests if the file is called directly.
if __name__ == '__main__':
    main()
