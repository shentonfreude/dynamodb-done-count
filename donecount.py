#!/usr/bin/env python3
"""Determine if we can do DB-enforced page counting and know we're done easily.

Can we do an update which increments pagecount and inserts the currently done
page into pagesdone list atomically, only if the page number doesn’t already
exist?

It should return the new page count, for free, as a consistent read IIRC.

If the count is equal to the total_pages, then the lambda can know we’re done,
and trigger a consolidation lambda.

This should be very fast, and enforce consistency.

Will the data become too large and cause a RCU/WCU slowdown if we have docs
with 1000 pages? 30,000 pages? Let’s say we store pages as a string, each of 5
bytes. 30000 pages * 5 bytes = 150KB. This is within the limit of DDB item size
but 150KB/4KB/RCU = 37.5 RCU.
"""

# in AWS_PROFILE=wp-dev
# pk=doc1, sk=na : count=42, done={1,3,5,7,11,13,17,19}
# TODO: make total_pages an attribute of this item

import boto3

DDB = 'cshenton-schema'
PK = 'doc1'
SK = 'na'

dbc = boto3.client('dynamodb')
dbr = boto3.resource('dynamodb')
dbt = dbr.Table(DDB)

pksk = {'pk': PK, 'sk': SK}
res = dbt.get_item(Key=pksk, ReturnConsumedCapacity='INDEXES')
item = res['Item']


res = dbt.update_item(
    Key=pksk, ReturnConsumedCapacity='INDEXES', ReturnValues='ALL_NEW',
    ExpressionAttributeNames={'#count': 'count'},
    ExpressionAttributeValues={':1': 1, ':mincount': 2},
    ConditionExpression="(#count > :mincount)",
    UpdateExpression="SET #count = #count + :1",
)
# botocore.errorfactory.ConditionalCheckFailedException

