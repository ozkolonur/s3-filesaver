import boto
import boto.s3.connection
import pdb
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import os
import dateutil.parser
import datetime

#aws_connection = S3Connection(access_key, secret_key)
#bucket = aws_connection.get_bucket('test902', validate=False)
#for file_key in bucket.list():
#    print file_key.name

#k = Key(bucket)
#k.key = 'foobar'
#k.set_contents_from_string("This is a test of S3");

#k = Key(bucket)
#k.key = 'test.txt'
#k.set_contents_from_file("./test.txt");

#key_name = 'tmp/test.txt'
#path = '/home/ubuntu/s3-upload'
#full_key_name = os.path.join(path, key_name)
#k = bucket.new_key(key_name)
#k.set_contents_from_filename("./test.txt")

def delete_old_files(access_key, secret_key, bucket_name, target_path, keep_last_days=5):
    aws_connection = S3Connection(access_key, secret_key)
    bucket = aws_connection.get_bucket(bucket_name, validate=False)
    past_date = datetime.datetime.now() + datetime.timedelta(minutes=-(keep_last_days))
    for file_key in bucket.list(prefix=target_path):
        print file_key.name, file_key.last_modified
        pdb.set_trace()
        file_creation_date = dateutil.parser.parse(file_key.last_modified)
        file_creation_date = file_creation_date.replace(tzinfo=None)
        if file_creation_date < past_date:
            print "I will delete"+file_key.name
            bucket.delete_key(file_key.name)

def upload_to_bucket(access_key, secret_key, bucket_name, local_file, target_path):
    aws_connection = S3Connection(access_key, secret_key)
    bucket = aws_connection.get_bucket(bucket_name, validate=False)
    filename = os.path.basename(local_file)
    if (target_path.endswith("/")):
        key_name = target_path + filename
    else:
        key_name = target_path + "/" + filename
    k = bucket.new_key(key_name)
    k.set_contents_from_filename(local_file)


delete_old_files(access_key = '',
                 secret_key = '',
                 bucket_name="",
                 target_path="abc",
                 keep_last_days = 30)
#upload_to_bucket(access_key = '',
#                 secret_key = '',
#                 bucket_name="",
#                 local_file="/home/ubuntu/s3-upload/test3.txt",
#                 target_path="/abc")



