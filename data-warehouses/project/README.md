Add the file `dwh.cfg` with the following content (do not check in to source control):
```editorconfig
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```

S3 bucket location:https://s3.console.aws.amazon.com/s3/buckets/udacity-dend