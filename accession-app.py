#!/usr/bin/env python

import logging
import sys

import config

from accessionprocessor import AccessionProcessor
from ingestapi import IngestApi
from listener import Listener


if __name__ == '__main__':
    log_format = ' %(asctime)s  - %(name)s - %(levelname)s in %(filename)s:%(lineno)s %(funcName)s(): %(message)s'
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_format)

    ingest_api = IngestApi(ingest_url=config.INGEST_API_URL)
    
    accession_processor = AccessionProcessor(ingest_api=ingest_api)

    listener = Listener({
        'rabbit': config.RABBITMQ_URL,
        'on_message_callback': accession_processor.run,
        'exchange': config.RABBITMQ_ACCESSION_EXCHANGE,
        'exchange_type': 'direct',
        'queue': config.RABBITMQ_ACCESSION_QUEUE,
        'routing_key': config.RABBITMQ_ACCESSION_QUEUE
    })

    listener.run()

