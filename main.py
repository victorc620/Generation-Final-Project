import pandas as pd
# from sqlalchemy import create_engine
from database_func import *
from data_normalisation_func import *
import numpy

# 1. Create different dataframe
df_original = load_csv_to_df('src/chesterfield_25-08-2021_09-00-00.csv')
df_transformed = basic_transform(df_original)

product_df = create_product_df(df_transformed)
location_df = create_location_df(df_transformed)
orders_df = create_orders_df(df_transformed, location_df)
orders_products_df = create_orders_products_df(df_transformed, product_df)

print(product_df)
print(location_df)
print(orders_df)
print(orders_products_df)


# # 2. Insert dataframe into database
# # create engine location: postgresql://username:password@host:port/database
# engine = create_engine('postgresql://team4gp:team4pw@localhost:5432/team4gp')

# insert_into_cafe(location_df, engine)
# insert_into_products(product_df, engine)
# insert_into_orders(orders_df, engine)
# insert_into_orders_products(orders_products_df, engine)


{'Records': [{'messageId': '57cf3705-6dac-4ef3-8e42-8b20a1071d65', 'receiptHandle': 'AQEBDCoWI2XfNSJLqKwhEFYHAQldQ/Ft33iqkr6AugUxOCcwIqb757rm/zV5YukeiCZn21sYrTGrgwOexIYG2/riEbDHkpv8IVOqxGZR876uEe7hS3Ywr/dDxHW8KfWHx/0xEQsBLX6xSHQ9CDJrHQP1CuX0c1A7tuial3XZUO67lRoguoUgyTc2cj9fiLOCEEIpHGe6xxY3qibsiwiAvj5OnsTSRzKmS5SyJ+HWCCa16R5PjA7Q1GZXCq/OJPb1xiwqLvszSRIQgnfwcx+buZ6Y/njf6L3VuSqerZTJGkbKSehFS3mdJBU1u3N6S/DlgQp8ObBQpWGMIpp01tT3ndKF1d9pv2QjIF2cmr0c0YIC8ThH5/OUKrRT5hRrQXui3qDHWKupj9oDSbkKAQNrzQBgbZe5bLjXDh+/HyMz/sJI54M=', 'body': '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"eu-west-1","eventTime":"2022-02-17T21:49:15.369Z","eventName":"ObjectCreated:Copy","userIdentity":{"principalId":"AWS:AROA2EDYES2FQFJUZEMB3:wai.yat.chu"},"requestParameters":{"sourceIPAddress":"152.37.121.88"},"responseElements":{"x-amz-request-id":"Q9JVRK6PZ0HDB723","x-amz-id-2":"dFJSXWHPWDzv04fOjMOdH5Dv3eJ7p/+yIgnU3JWZEojgJNQ0yyvLMfIW3M/Y048qDrxzpRoPXv1XAlKhoYY9JUcbrRvp+lD5"},"s3":{"s3SchemaVersion":"1.0","configurationId":"transformed-csv-loaded-to-s3","bucket":{"name":"team4-transformed-data-bucket","ownerIdentity":{"principalId":"A28NEUIOUXKZ9Y"},"arn":"arn:aws:s3:::team4-transformed-data-bucket"},"object":{"key":"location_df.csv","size":63,"eTag":"96f4b7ad7ed70daa52e9d4fbdf75c218","sequencer":"00620EC2DB56FC81C9"}}}]}', 'attributes': {'ApproximateReceiveCount': '4', 'SentTimestamp': '1645134556264', 'SenderId': 'AIDAJQOC3SADRY5PEMBNW', 'ApproximateFirstReceiveTimestamp': '1645134556264'}, 'messageAttributes': {}, 'md5OfBody': '9c8570da1369805e3cfebbfaa8b7cc83', 'eventSource': 'aws:sqs', 'eventSourceARN': 'arn:aws:sqs:eu-west-1:696036660875:team-4-load-to-redshift-queue', 'awsRegion': 'eu-west-1'}]}
{'Records': [{'messageId': '57cf3705-6dac-4ef3-8e42-8b20a1071d65', 'receiptHandle': 'AQEBDCoWI2XfNSJLqKwhEFYHAQldQ/Ft33iqkr6AugUxOCcwIqb757rm/zV5YukeiCZn21sYrTGrgwOexIYG2/riEbDHkpv8IVOqxGZR876uEe7hS3Ywr/dDxHW8KfWHx/0xEQsBLX6xSHQ9CDJrHQP1CuX0c1A7tuial3XZUO67lRoguoUgyTc2cj9fiLOCEEIpHGe6xxY3qibsiwiAvj5OnsTSRzKmS5SyJ+HWCCa16R5PjA7Q1GZXCq/OJPb1xiwqLvszSRIQgnfwcx+buZ6Y/njf6L3VuSqerZTJGkbKSehFS3mdJBU1u3N6S/DlgQp8ObBQpWGMIpp01tT3ndKF1d9pv2QjIF2cmr0c0YIC8ThH5/OUKrRT5hRrQXui3qDHWKupj9oDSbkKAQNrzQBgbZe5bLjXDh+/HyMz/sJI54M=', 'body': '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"eu-west-1","eventTime":"2022-02-17T21:49:15.369Z","eventName":"ObjectCreated:Copy","userIdentity":{"principalId":"AWS:AROA2EDYES2FQFJUZEMB3:wai.yat.chu"},"requestParameters":{"sourceIPAddress":"152.37.121.88"},"responseElements":{"x-amz-request-id":"Q9JVRK6PZ0HDB723","x-amz-id-2":"dFJSXWHPWDzv04fOjMOdH5Dv3eJ7p/+yIgnU3JWZEojgJNQ0yyvLMfIW3M/Y048qDrxzpRoPXv1XAlKhoYY9JUcbrRvp+lD5"},"s3":{"s3SchemaVersion":"1.0","configurationId":"transformed-csv-loaded-to-s3","bucket":{"name":"team4-transformed-data-bucket","ownerIdentity":{"principalId":"A28NEUIOUXKZ9Y"},"arn":"arn:aws:s3:::team4-transformed-data-bucket"},"object":{"key":"location_df.csv","size":63,"eTag":"96f4b7ad7ed70daa52e9d4fbdf75c218","sequencer":"00620EC2DB56FC81C9"}}}]}', 'attributes': {'ApproximateReceiveCount': '4', 'SentTimestamp': '1645134556264', 'SenderId': 'AIDAJQOC3SADRY5PEMBNW', 'ApproximateFirstReceiveTimestamp': '1645134556264'}, 'messageAttributes': {}, 'md5OfBody': '9c8570da1369805e3cfebbfaa8b7cc83', 'eventSource': 'aws:sqs', 'eventSourceARN': 'arn:aws:sqs:eu-west-1:696036660875:team-4-load-to-redshift-queue', 'awsRegion': 'eu-west-1'}]}