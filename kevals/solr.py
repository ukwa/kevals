'''
Functions for interacting with Solr, as a Tracking Database.

NOTE requires Solr > 7.3 as uses 'add-distinct'

See https://lucene.apache.org/solr/guide/7_3/updating-parts-of-documents.html

'''
import requests
import logging
import json
import os

logger = logging.getLogger(__name__)

class SolrKevalsDB():

    def __init__(self, kevalsdb_url=os.environ.get('KEVALS_SOLR_URL', None), update_batch_size=1000):
        if not kevalsdb_url:
            raise Exception("You must supply a KEVALS_SOLR_URL!")
        # Record settings:
        self.kevalsdb_url = kevalsdb_url
        self.batch_size = update_batch_size
        # Set up the update configuration:
        self.update_kevalsdb_url = self.kevalsdb_url + '/update?softCommit=true'

    def _jsonl_doc_generator(self, input_reader):
        for line in input_reader:
            item = json.loads(line)
            # And return
            yield item

    def _send_batch(self, batch, as_updates=True):
        # Convert the plain dicts into Solr update documents:
        updates = []
        for item in batch:
            # There must be an ID, so complain if there isn't.
            if not 'id' in item:
                raise Exception("You should supply an id for each update! This update has no ID: %s" % item )
            # Turn into an update:
            update_item = {}
            for key in item:
                if key == 'id':
                    update_item[key] = item[key]
                elif key == '_version_':
                    # Do nothing, as we don't want to send that, because it'll cause conflicts on import.
                    pass
                else:
                    # If we want to send updates, except those already arranged as updates (i.e. as dicts):
                    if as_updates and not isinstance(item[key], dict):
                        # Convert to 'set' updates:
                        update_item[key] = { 'set': item[key] }
                    else:
                        update_item[key] = item[key]
            # Add the item to the set:
            updates.append(update_item)

        # And post the batch as updates:
        self._send_update(updates)

    def import_jsonl_reader(self, input_reader):
        self.import_items_from(self._jsonl_doc_generator(input_reader))

    def import_items(self, items):
        self._send_batch(items)

    def import_items_from(self, item_generator):
        batch = []
        for item in item_generator:
            batch.append(item)
            if len(batch) > self.batch_size:
                self._send_batch(batch)
                batch = []
        # And send the final batch if there is one:
        if len(batch) > 0:
            self._send_batch(batch)

    def list(self, field_value=None, sort='timestamp_dt desc', limit=100):
        # set solr search terms
        solr_query_url = self.kevalsdb_url + '/query'
        query_string = {
            'q':'*:*',
            'rows':limit,
            'sort':sort
        }
        # Add optional fields:
        if field_value:
            if field_value[1] == '_NONE_' or field_value[1] == '':
                query_string['q'] += ' AND -{}:[* TO *]'.format(field_value[0])
            else:
                query_string['q'] += ' AND {}:{}'.format(field_value[0], field_value[1])
        # gain tracking_db search response
        logger.info("SolrTrackDB.list: %s %s" %(solr_query_url, query_string))
        r = requests.post(url=solr_query_url, data=query_string)
        if r.status_code == 200:
            response = r.json()['response']
            # return hits, if any:
            if response['numFound'] > 0:
                return response['docs']
            else:
                return []
        else:
            raise Exception("Solr returned an error! HTTP %i\n%s" %(r.status_code, r.text))

    def get(self, id):
        # set solr search terms
        solr_query_url = self.kevalsdb_url + '/query'
        query_string = {
            'q':'id:"{}"'.format(id)
        }
        # gain tracking_db search response
        logger.info("SolrTrackDB.get: %s %s" %(solr_query_url, query_string))
        r = requests.post(url=solr_query_url, data=query_string)
        if r.status_code == 200:
            response = r.json()['response']
            # return hits, if any:
            if response['numFound'] == 1:
                return response['docs'][0]
            else:
                return None
        else:
            raise Exception("Solr returned an error! HTTP %i\n%s" %(r.status_code, r.text))

    def _send_update(self, post_data):
        # Covert the list of docs to JSONLines:
        #post_data = ""
        #for item in docs:
        #    post_data += ("%s\n" % json.dumps(item))
        # Set up the POST and check it worked
        post_headers = {'Content-Type': 'application/json'}
        logger.info("SolrTrackDB.update: %s %s" %(self.update_kevalsdb_url, str(post_data)[0:1000]))
        r = requests.post(url=self.update_kevalsdb_url, headers=post_headers, json=post_data)
        if r.status_code == 200:
            response = r.json()
        else:
            raise Exception("Solr returned an error! HTTP %i\n%s" %(r.status_code, r.text))

    def _update_generator(self, ids, field, value, action):
        for id in ids:
            # Update TrackDB record for records based on ID:
            yield { 'id': id, field: { action: value } } 
        
    def update(self, ids, field, value, action='add-distinct'):
        self.import_items_from(self._update_generator(ids, field, value, action))

