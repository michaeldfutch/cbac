# utils 
import pandas_gbq
import pandas as pd
import json
import requests
from requests.auth import HTTPBasicAuth


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

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"
    from google.cloud import storage
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    
    metadata = {"Cache-control":"max-age=0"}
    blob.metadata = metadata
    blob.patch()
    
    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

def update_posts(CLIENT_KEY, CLIENT_SECRET):

    try:
        max_mod_date = pandas_gbq.read_gbq('select max(modified) from cbac_wordpress.wp_posts_raw_v2', progress_bar_type=None).iloc[0]

        if pd.isnull(max_mod_date[0]):
            max_mod_date = '2021-10-27'
            page_size = 50
        else:
            max_mod_date = str(max_mod_date[0])
            page_size = 10
    except:
        max_mod_date = '2021-10-27'
        page_size = 50


    url = f'https://cbavalanchecenter.org/wp-json/wp/v2/posts/?_fields=id,title,link,date,modified,content'

    page = 1

    while True:
        r = requests.get(url=url, auth=HTTPBasicAuth(CLIENT_KEY, CLIENT_SECRET), 
                         json={'page': page,'per_page':page_size,
                              'order':'desc',
                               'orderby':'modified'
                              }
            )
        content = json.loads(r._content.decode('utf-8'))   
        page += 1
        if len(content) == 0 or r.status_code == 400:
            break
        else:
            posts_df = pd.json_normalize(content)
            posts_df.columns = ['id','date','modified','url','title','content','content_protected']
            posts_df = posts_df[posts_df['modified'].astype(str) > max_mod_date]
            pandas_gbq.to_gbq(posts_df, destination_table='cbac_wordpress.wp_posts_raw_v2', if_exists='append', progress_bar=None)
            print("Loaded", posts_df.shape[0], "posts")
            
            if posts_df.shape[0] == 0 or posts_df['modified'].min() < max_mod_date:
                print("Breaking: No more updates")
                break