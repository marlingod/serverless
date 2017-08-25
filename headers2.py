from __future__ import print_function
import boto3
import os
import sys
import uuid
import tarfile
import re

# download each key in temp folder of the lambda function
#process apply a tarfile action to it
# select the column_headers.tsv file and renamed it column_headers_date_time.tsv.

# upload the new file in separate folder

def py_files(members):
    for tarinfo in members:
        if os.path.splitext(tarinfo.name)[0] == "column_headers":
            yield tarinfo

s3 = boto3.resource('s3')

# GET THE BUCKET NAME

BUCKET = s3.Bucket('bucketname')
BUCKET_COL = s3.Bucket('bucketname2)

for key in BUCKET.objects.all():
        key =str(key.key)
        download_path = '/tmp/{}{}'.format(uuid.uuid4(),key)
        pattern  = re.compile("sdc-nmglobaldata_([^\/]*)-lookup_data.tar.gz$")
        filetime = str(pattern.match(key).group(1))
        # download the tar.gz in the temp folder
        new_key_name = str(download_path +'/'+key)
        BUCKET.download_file(key,download_path)
        tar = tarfile.open(download_path)
        downlo = '/tmp/{}'.format(uuid.uuid4())
        tar.extractall(path=downlo,members=py_files(tar))
        tar.close()
        newfile_name = "column_headers_{}.tsv".format(filetime)
        os.rename(downlo+"/"+ "column_headers.tsv", downlo+"/"+newfile_name)
        BUCKET_COL.upload_file(downlo+"/"+newfile_name, newfile_name)
