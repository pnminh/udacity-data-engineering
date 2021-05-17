## Create EMR
```shell
$ aws ec2 create-key-pair --key-name <SSH_KEYPAIR_NAME>

$ aws emr create-cluster --name spark-udacity
--name <CLUSTER_NAME> 
--use-default-roles  
--release-label emr-5.28.0 
--instance-count 2 
--applications Name=Spark  
--ec2-attributes KeyName=<SSH_KEYPAIR_NAME> 
--instance-type m5.xlarge 
--instance-count 2 
--auto-terminate`

aws emr create-cluster --name emr --use-default-roles --release-label emr-5.28.0 --instance-count 2 --instance-type m5.xlarge --applications Name=Spark Name=Hadoop Name=Livy Name=Hive --ec2-attributes KeyName=emr --log-uri s3://minh-emr-log

#result
{
    "ClusterId": "j-1XHWPT46O2H10",
    "ClusterArn": "arn:aws:elasticmapreduce:us-east-1:756983811816:cluster/j-1XHWPT46O2H10"
}

```
## Note:
- Need to open ssh in security group for ssh
- Error with s3: https://stackoverflow.com/questions/66898198/spark-read-data-from-s3-how-to-config-fs-s3a-multipart-size-correctly-for-a-s