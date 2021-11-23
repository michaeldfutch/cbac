import requests
import pandas as pd
import json
from requests.auth import HTTPBasicAuth
import pandas_gbq
import logging
import config
import argparse
import logging
import pyarrow
from gspread_dataframe import set_with_dataframe
import gspread
from google.oauth2 import service_account

## DEPLOY
## gcloud functions deploy form_12 --entry-point main --runtime python37 --trigger-resource cbac_topic_3 --trigger-event google.pubsub.topic.publish --timeout 540s
## gcloud scheduler jobs create pubsub cbac_nightly --schedule "5 4 * * *" --topic cbac_topic_3 --message-body "form 12 submission data" --time-zone "America/Denver"


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
    return string.lower().replace(' ','_').replace("'",'').replace('?','').replace('(','').replace(')','').replace('.','')


def file_to_string(sql_path):
    """Converts a SQL file holding a SQL query to a string.
    Args:
        sql_path: String containing a file path
    Returns:
        String representation of a file's contents
    """
    with open(sql_path, 'r') as sql_file:
        return sql_file.read()


def main(data, context):
    
    # set api key
    CLIENT_KEY = config.config_vars['CLIENT_KEY']
    CLIENT_SECRET = config.config_vars['CLIENT_SECRET']
    PROJECT_ID = config.config_vars['project_id']
    KEYFILE =  config.config_vars['credentials']
    #credentials = service_account.Credentials.from_service_account_file(config.config_vars['credentials'])

    df = None
    url = 'https://cbavalanchecenter.org/wp-json/gf/v2/forms/12/entries?_labels=1'
    sql = "select max(date_updated) max_date from cbac_wordpress.obs_form_12_direct"

    # gather entries until we start to get old ones
    page_size = 20
    entries = []
    page = 1
    while True:
        r = requests.get(url=url, auth=HTTPBasicAuth(CLIENT_KEY, CLIENT_SECRET), json={'paging': 
                                                                                            {'current_page': page,
                                                                                            'page_size':page_size },
                                                                                        'sorting':
                                                                                            {'key':'date_updated',
                                                                                            'direction':'ASC'}})
        

        content = json.loads(r._content.decode('utf-8'))
        entries.extend(content['entries'])
        page += 1    
        if len(content['entries']) == 0:
            break

    labels = content['_labels']
    flat_labels = flatten_dict(labels)


    ## loop through the entries to build a dataframe
    loop = 0   
    for entry in entries:
        
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
        pandas_gbq.to_gbq(df, 'cbac_wordpress.obs_form_12_direct', project_id=PROJECT_ID, if_exists='replace')


    ## TODO: Check for posts instead of using the zap and replace `cbac-306316.cbac_wordpress.wp_posts_view` in query below
    
    # rebuild endpoint table    
    sql = file_to_string(config.config_vars['sql_file_path'])
    endpoint_table  =  pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)
    pandas_gbq.to_gbq(endpoint_table, 'cbac_wordpress.observation_endpoint', project_id=PROJECT_ID, if_exists='replace')

    # rebuild long avy table table    
    sql = file_to_string(config.config_vars['sql_file_path_2'])
    long_table  =  pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)
    pandas_gbq.to_gbq(long_table, 'cbac_wordpress.long_avy_table', project_id=PROJECT_ID, if_exists='replace')


    ## Export latest tables to sheets
    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']
    credentials = service_account.Credentials.from_service_account_file(KEYFILE, scopes = scope)

    gc = gspread.authorize(credentials)

    spreadsheet_key = '17VYlXBhxE0NkOJ3p3uepqub8tBkRb3P1en0dhRd3w8A'
    sh = gc.open_by_key(spreadsheet_key)
    obs_sheet = sh.get_worksheet(0)
    endpoint_sheet = sh.get_worksheet(1)
    long_sheet = sh.get_worksheet(2)
    # get full df
    form_12 = pandas_gbq.read_gbq('select * except(email, first, last, ip) from cbac_wordpress.obs_form_12_direct order by id')
    set_with_dataframe(obs_sheet, form_12, resize=True)

    # observation_endpoint
    # get full df
    obs_endpoint = pandas_gbq.read_gbq('select * from cbac_wordpress.observation_endpoint order by entry_id')
    set_with_dataframe(endpoint_sheet, obs_endpoint, resize=True)

    # avy_long_format
    # get full df
    avy_long_format = pandas_gbq.read_gbq('select * from cbac_wordpress.long_avy_table order by estimated_avalanche_date, forecast_zone, location')
    set_with_dataframe(long_sheet, avy_long_format, resize=True)

if __name__ == '__main__':
    main('data', 'context')