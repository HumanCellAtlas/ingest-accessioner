import json
import logging
import sys
import uuid

log_format = ' %(asctime)s  - %(name)s - %(levelname)s in %(filename)s:%(lineno)s %(funcName)s(): %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_format)


class AccessionProcessor:
    def __init__(self, ingest_api):
        self.ingest_api = ingest_api
        self.logger = logging.getLogger(__name__)

    def run(self, message):
        params = json.loads(message)

        metadata_uuid = params['documentUuid']

        if not metadata_uuid:
            new_uuid = str(uuid.uuid4())
            debug_message = 'New Accession Number {new_uuid}'.format(new_uuid=new_uuid)
            self.logger.info(debug_message)

            metadata_update = {'uuid': {}}
            metadata_update['uuid']['uuid'] = new_uuid

            entity_link = params['callbackLink']

            if self.ingest_api.update_entity_if_match(entity_link, json.dumps(metadata_update)):
                self.logger.info('updated entity accession uuid!')
            else:
                self.logger.info('no update')
        else:
            self.logger.info('no update')
