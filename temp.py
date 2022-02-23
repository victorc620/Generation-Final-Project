import json

event = {'Records': [{'messageId': 'e07ce73b-04fe-4633-9c44-5d6d823660aa', 'receiptHandle': 'AQEBi8qGW69cYPmXavadR0qKCY5+tBXD7QhUrmlzUFo9oiy9p3hozWvoqz6Mea06lYgnWCeBqScx1N4hme4A03+E6KsqM9oPwF3taqhv7lkqqLSy+rfvTk4ONlYbvmX4l4kyaUQdetDsXq/hwGOI+umdnWuB2QbYkmuKyBg/ysn084iUoBOwVoI8GKKUWOwWNrBpkQnuja+Jcl8ZtaQs2bQZ9/0CItIjnbaVlxkckVEALBMbqBxAq2jOHR9c2QqhMNxcfZe3mfgbVRZkleUei85zwT1zAfRSjTntShRtHKgyEFL2g7QYOjxZTEi/zCceXMmfzu2X3nybIc7Zfdtlwo3PL1WDC+C6ECXBnmgqPDXrhIyB0QrnQUSKpY6kIbGtdzDDbRaXeO/PTtzNvcvFruITIN4Nec8gq5uH5IcQ8S0hl8s=', 'body': '{"Service":"Amazon S3","Event":"s3:TestEvent","Time":"2022-02-23T10:10:27.388Z","Bucket":"team-4-transformed-data-production","RequestId":"C6HVKS6MKW4R43NT","HostId":"/gm4Bqpv2I1IZDzDAUmmfWby9dHi5Qr36Yp5eMZLhL4UEysG2B5+JCvJ8XBLmg8Xa2TV7GoRdR8="}', 'attributes': {'ApproximateReceiveCount': '214', 'SentTimestamp': '1645611027422', 'SenderId': 'AIDAJQOC3SADRY5PEMBNW', 'ApproximateFirstReceiveTimestamp': '1645611027422'}, 'messageAttributes': {}, 'md5OfBody': '725e334f40cdcdb57fca6a2d575cb59f', 'eventSource': 'aws:sqs', 'eventSourceARN': 'arn:aws:sqs:eu-west-1:696036660875:team-4-sqs-queue-production', 'awsRegion': 'eu-west-1'}]}

sqs_msg = event['Records'][0]["body"]
sqs_msg = json.loads(sqs_msg)

s3_bucket_name = sqs_msg["Bucket"]
# s3_file_key = sqs_msg["Records"][0]["s3"]["object"]["key"]

print(sqs_msg)
print(s3_bucket_name)