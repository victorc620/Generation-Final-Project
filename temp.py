import json

event = {'Records': [{'messageId': 'ec3788d2-85b5-47bd-8a9d-682ba412017e', 'receiptHandle': 'AQEB8uyIWUX8vBDkUoIDg9H7lt3J6YuZVVbMY6NBrxSVA2LuOhhfIE+0cT07cQaS28GMnp/fFKQh7dwy4sXt3C9aj1J3g61YsOaOvB6QizCwLf/7Wy13X9mRSmZCT3IIY/sGTkwP4Z3S0F5VweXvbcVekzu5anMMkGef0qjV6Uxj1y3qTJVTSvPBJWdRT6YRVdbgF/lQwubYXX4xMEp9GmE98lD3PreiiQuA9KF2p2YUHRrZy1P2R2FNZY1W8Vd2lDOO94mel9zbK1zw7apCsfp8dZ9y4K5t4pdSk2QV2bVwiB3ZblIAM5Hb1ShsiKVoN44eTSFk/w0f54ZRX1aswIKRdm+JEURqU2XObWB49MGRHrtlXRUkC2fSGL63cVXX4YoOMPVOzzmnsNvdR1K15CuO+A==', 'body': '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"eu-west-1","eventTime":"2022-02-23T11:50:18.561Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AROA2EDYES2F5ZLKMLWXK:team-4-extract-transform-production"},"requestParameters":{"sourceIPAddress":"34.251.246.125"},"responseElements":{"x-amz-request-id":"491T3NYSDS9ZWCPV","x-amz-id-2":"7ei+svf9EZLB6SLjOFZpN5/Ucetj516OHzCt+GS1JaYnsE1uinMgNLU3Hjm8ofyJ4/I9hsnH7XYf4q1gcpUHTGUIemyfsNLc"},"s3":{"s3SchemaVersion":"1.0","configurationId":"a12c8124-0d12-4242-8241-c4bdd5e85156","bucket":{"name":"team-4-transformed-data-production","ownerIdentity":{"principalId":"A28NEUIOUXKZ9Y"},"arn":"arn:aws:s3:::team-4-transformed-data-production"},"object":{"key":"orders_df.test_data.csv","size":25731,"eTag":"56c81da066f8ee748a74aeba0c39bfd0","sequencer":"0062161F7A821E52D3"}}}]}', 'attributes': {'ApproximateReceiveCount': '35', 'SentTimestamp': '1645617019686', 'SenderId': 'AIDAJQOC3SADRY5PEMBNW', 'ApproximateFirstReceiveTimestamp': '1645617019686'}, 'messageAttributes': {}, 'md5OfBody': '78415b0616f51a348af66075ecd46834', 'eventSource': 'aws:sqs', 'eventSourceARN': 'arn:aws:sqs:eu-west-1:696036660875:team-4-sqs-queue-production', 'awsRegion': 'eu-west-1'}]}


sqs_msg = event['Records'][0]["body"]
sqs_msg = json.loads(sqs_msg)

s3_bucket_name = sqs_msg["Records"][0]["s3"]["bucket"]["name"]
s3_file_key = sqs_msg["Records"][0]["s3"]["object"]["key"]

print(sqs_msg)
print(s3_bucket_name)
print(s3_file_key)