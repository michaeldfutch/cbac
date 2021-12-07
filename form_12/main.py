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
import matplotlib.pyplot as plt
import datetime
import numpy as np
from matplotlib import cm
from utils import *


## DEPLOY
## gcloud functions deploy form_12 --entry-point main --runtime python37 --trigger-resource cbac_topic_3 --trigger-event google.pubsub.topic.publish --timeout 540s --memory 1024MB
## gcloud scheduler jobs create pubsub cbac_nightly --schedule "5 4 * * *" --topic cbac_topic_3 --message-body "form 12 submission data" --time-zone "America/Denver"


parser = argparse.ArgumentParser()
parser.add_argument( '-log',
                     '--loglevel',
                     default='warning',
                     help='Provide logging level. Example --loglevel debug, default=warning' )

args = parser.parse_args()

logging.basicConfig( level=args.loglevel.upper() )
logging.info( 'Logging now setup.' )


def main(data, context):
    
    # set api key
    CLIENT_KEY = config.config_vars['CLIENT_KEY']
    CLIENT_SECRET = config.config_vars['CLIENT_SECRET']
    PROJECT_ID = config.config_vars['project_id']
    KEYFILE =  config.config_vars['credentials']

    df = None
    url = 'https://cbavalanchecenter.org/wp-json/gf/v2/forms/12/entries?_labels=1'


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


    # expand dataframe based on number_of_avalanches

    time_cutoff = 21
    avy_long_format['days_old'] = (time_cutoff-(pd.to_datetime("today") - pd.to_datetime(avy_long_format['estimated_avalanche_date'])) / np.timedelta64(1, 'D'))/time_cutoff
    avy_long_format['days_old'] = avy_long_format['days_old'].mask(avy_long_format['days_old'] < 0, 0)

    expanded = pd.DataFrame()
    for index, row in avy_long_format.iterrows():
        if row['days_old'] > 0:
            for i in range(int(row['number_of_avalanches'])):
                expanded = pd.concat([expanded, pd.DataFrame(row).transpose()], axis = 0, ignore_index=True)
    avy_long_format = expanded

    ### plot avys
    r_grids = (1.5,2.25)

    r_anchors = {'BTL': (3+r_grids[1])/2, 'NTL': (r_grids[1]+r_grids[0])/2, 'ATL': 2/3 * r_grids[0]}
    thetas = []
    dirs = ['E','NE','N','NW','W','SW','S','SE']
    distances = [3.3,3.3,3.1,3.3,3.3,3.3,3.3,3.3]
    align=['right','center','center','center','left','center','center','center']
    x_anchors = {}
    for i in range(8):
        thetas.append(22.5 + 45 * (i))
        x_anchors[dirs[i]] =  45 * (i)
    
    sizes = {'D1':10,
         'D1.5':50,
         'D2':100,
         'D2.5':200,
         'D3':400,
         'D3.5':600,
         'D4':800,
         'D4.5':1000,
         'D5':1200}

    avy_long_format['polar_x'] = avy_long_format['aspect'].apply(lambda x: np.deg2rad(x_anchors[x]) + np.random.normal()*.1)
    avy_long_format['polar_y'] = avy_long_format['start_zone_elevation'].apply(lambda x: r_anchors[x]+ np.random.normal()*.15)
    avy_long_format['point_size'] = avy_long_format['destructive_size'].apply(lambda x: sizes[x])
    

    days = datetime.timedelta(time_cutoff)
    today = pd.Timestamp.date(pd.to_datetime("today"))
    begin = today - days
    title = f'''CBAC Avalanche Observations
    {begin} to {today}'''


    fig = plt.figure(figsize=(8, 8))
    ax = fig.add_subplot(projection='polar')

    x, y, s, c = avy_long_format['polar_x'], avy_long_format['polar_y'], avy_long_format['point_size'], avy_long_format['days_old']

    ax.set_yticklabels([])
    ax.set_rgrids(r_grids)
    ax.set_ylim(0,3)
    scatter = ax.scatter( x, y, s, c=c, cmap=cm.GnBu, marker='o', vmin=0, vmax=1)
    cbar = fig.colorbar(scatter, ticks=[0, 1], fraction=0.04)
    cbar.ax.set_yticklabels(['Less Recent', 'More Recent'])# vertically oriented colorbar

    d_labels = ["D1", "D2", "D3", "D4", "D5"]
    markers = []
    for i in range(len(d_labels)):
        markers.append(plt.scatter([],[], s=sizes[d_labels[i]], label=d_labels[i], color="lightblue"))

    plt.legend(handles=markers, loc="lower center", ncol=5, bbox_to_anchor=(.5, -.15), frameon=False)

    ax.set_thetagrids(tuple(thetas), labels = [])

    tick = [ax.get_rmax(),ax.get_rmax()*0.97]
    i = 0
    for t  in np.deg2rad(np.arange(0,360,45)):
        ax.plot([t,t], tick, lw=0.72, color="k")
        plt.text(t,distances[i],dirs[i], snap=True, horizontalalignment = align[i], fontsize=12)
        i+=1
        
    plt.title(title, y=1.0, pad=25)
    plt.tight_layout()
    
    filename = 'avys.jpg'
    source_file_name = '/tmp/' + filename
    

    plt.savefig(source_file_name, format='jpg' )
    plt.close()

    bucket_name = 'cbac-306316.appspot.com'

    upload_blob(bucket_name, source_file_name, filename)

if __name__ == '__main__':
    main('data', 'context')