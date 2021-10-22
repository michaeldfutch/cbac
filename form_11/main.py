import requests
import pandas as pd
import json
from requests.auth import HTTPBasicAuth
import pandas_gbq
import datetime
import logging
from string import Template
import config
from google.cloud import bigquery
import argparse
import logging
import pyarrow


## DEPLOY
## gcloud functions deploy form_11 --entry-point main --runtime python37 --trigger-resource cbac_topic_2 --trigger-event google.pubsub.topic.publish --timeout 540s
## gcloud scheduler jobs create pubsub cbac_nightly --schedule "5 4 * * *" --topic cbac_topic_2 --message-body "rebuild form 11 submission data" --time-zone "America/Denver"


parser = argparse.ArgumentParser()
parser.add_argument( '-log',
                     '--loglevel',
                     default='warning',
                     help='Provide logging level. Example --loglevel debug, default=warning' )

args = parser.parse_args()

logging.basicConfig( level=args.loglevel.upper() )
logging.info( 'Logging now setup.' )


### helper functions
def flatten_dict(D):
    new_dict = {}
    for key, value in D.items():
        if type(value) == dict:
            for k, v in value.items():
                new_dict[k] = v
        else:
            new_dict[key] = value
    return new_dict

def clean(string):
    return string.lower().replace(' ','_').replace("'",'').replace('?','')

def file_to_string(sql_path):
    """Converts a SQL file holding a SQL query to a string.
    Args:
        sql_path: String containing a file path
    Returns:
        String representation of a file's contents
    """
    with open(sql_path, 'r') as sql_file:
        return sql_file.read()

def execute_query(bq_client):
    """Executes transformation query to a new destination table.
    Args:
        bq_client: Object representing a reference to a BigQuery Client
    """
    dataset_ref = bq_client.get_dataset(bigquery.DatasetReference(
        project=config.config_vars['project_id'],
        dataset_id=config.config_vars['output_dataset_id']))
    table_ref = dataset_ref.table(config.config_vars['output_table_name'])
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition().WRITE_TRUNCATE
    sql = file_to_string(config.config_vars['sql_file_path'])
    logging.info('Attempting query on all dates...')
    # Execute Query
    query_job = bq_client.query(
        sql,
        job_config=job_config)

    query_job.result()  # Waits for the query to finish
    logging.info('Query complete. The table is updated.')


def main(data, context):
    
    # set api key
    CLIENT_KEY = config.config_vars['CLIENT_KEY']
    CLIENT_SECRET = config.config_vars['CLIENT_SECRET']
    PROJECT_ID = config.config_vars['project_id']
    #credentials = service_account.Credentials.from_service_account_file(config.config_vars['credentials'])

    df = None
    url = 'https://dev.cbavalanchecenter.org/wp-json/gf/v2/forms/11/entries?_labels=1'

    sql = "select max(id) max_id from cbac_wordpress.obs_form_11_direct"

    # find the max current id if it exists
    try:
        max_id = int(pandas_gbq.read_gbq(sql, project_id=PROJECT_ID).iloc[0]['max_id'])
    except:
        logging.info("Table obs_form_11_direct does not exist, building from scratch")
        max_id = 0

    # gather entries until we start to get old ones
    page_size = 20
    entries = []
    page = 1
    while True:
        r = requests.get(url=url, auth=HTTPBasicAuth(CLIENT_KEY, CLIENT_SECRET), json={'paging': 
                                                                                        {'current_page': page,
                                                                                        'page_size':page_size }})
        

        content = json.loads(r._content.decode('utf-8'))
        entries.extend(content['entries'])
        page += 1    
        if len(content['entries']) == 0:
            break
        if int(entries[-1]['id']) <= max_id:
            break

    labels = content['_labels']
    flat_labels = flatten_dict(labels)


    ## loop through the entries to build a dataframe
    loop = 0   
    for entry in entries:
        if int(entry['id']) <= max_id:
            logging.info("Stopping: Remaining data already loaded")
            break
        
        form_fields = {}

        # dict to keep track of number of times keys have been used
        counter_dict = {}
        for key, value in flat_labels.items():
            counter_dict[clean(value)] = 0

        for key, value in entry.items():
            try:
                ## Need to iterate key names when labels are repeated e.g. aspect1, aspect2 etc
                if clean(flat_labels[key]) in form_fields:
                    counter_dict[clean(flat_labels[key])] += 1
                    form_fields[clean(flat_labels[key] + '_' + str(counter_dict[clean(flat_labels[key])]))] = value
                else:
                    form_fields[clean(flat_labels[key])] = value

            except:
                form_fields[clean(key)] = value
        
        # pandas does not like empty lists as values
        form_fields['photo_gallery'] = [form_fields['photo_gallery']]
        
        if loop == 0:
            df = pd.DataFrame(form_fields, index = [loop,])
        else: 
            df = df.append(pd.DataFrame(form_fields, index = [loop,]))
        
        loop += 1

    # append to existing table or build it if it doesn't exist
    if df is not None:
        pandas_gbq.to_gbq(df, 'cbac_wordpress.obs_form_11_direct', project_id=PROJECT_ID, if_exists='append')


    ## TODO: Check for posts instead of using the zap and replace `cbac-306316.cbac_wordpress.wp_posts_view` in query below
    
    # rebuild endpoint table
    bq_client = bigquery.Client()

    try:
        current_time = datetime.datetime.utcnow()
        log_message = Template('Cloud Function was triggered on $time')
        logging.info(log_message.safe_substitute(time=current_time))

        try:
            execute_query(bq_client)

        except Exception as error:
            log_message = Template('Query failed due to '
                                   '$message.')
            logging.error(log_message.safe_substitute(message=error))

    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)


if __name__ == '__main__':
    main('data', 'context')