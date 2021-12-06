# utils 


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