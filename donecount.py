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

from pprint import pprint as pp
from random import randint

import boto3

DDB = 'cshenton-schema'
PK1 = 'doc1'                    # NS { 1, 2, 3}
PK2 = 'doc2'                    # L  [ {"S": "42"}, {"S": "6"}]
SK = 'na'

dbc = boto3.client('dynamodb')
dbr = boto3.resource('dynamodb')
dbt = dbr.Table(DDB)

res = dbt.get_item(Key={'pk': 'doc1', 'sk': 'na'}, ReturnConsumedCapacity='INDEXES')
item = res['Item']

# Can raise botocore.errorfactory.ConditionalCheckFailedException

page = randint(0, 1000)


# try update SN set of numbers
res = dbt.update_item(
    Key={'pk': 'doc1', 'sk': 'na'},
    ReturnConsumedCapacity='INDEXES', ReturnValues='ALL_NEW',
    ExpressionAttributeNames={
        '#count': 'count',
        # '#page': 'page'},
        '#done': 'done',
    },
    ExpressionAttributeValues={
        ':1': 1,
        ':page': page,
        ':pagelist': set([page]),  # set([page])
    },
    ConditionExpression="(NOT contains(done, :page))",
    UpdateExpression="ADD #done :pagelist SET #count = #count + :1",

    # I can add to the numset, or increment the count but not both
    #UpdateExpression="SET #count = #count + :1 ADD #done :page",
    #UpdateExpression="SET #count = #count + :1, done = list_append(#done, :pagelist)",
    #UpdateExpression="ADD #done :pagelist"  # , SET #count = #count + :1",
    #UpdateExpression="SET #count = #count + :1, #done :pagelist"
)
print('rand int page=%s' % page)
pp(res)

exit(0)

###############################################################################
# Show the LIST (of random types) works:
page_str = str(page)
res = dbt.update_item(
    Key={'pk': 'doc2', 'sk': 'na'},
    ReturnConsumedCapacity='INDEXES', ReturnValues='ALL_NEW',
    ExpressionAttributeNames={'#count': 'count',
                              '#done': 'done'},
    ExpressionAttributeValues={':1': 1,
                               ':page': str(page),
                               ':pagelist': [str(page)]},
    ConditionExpression="(NOT contains(done, :page))",
    UpdateExpression="SET #count = #count + :1, done = list_append(#done, :pagelist)",
)
print('rand str page=%s' % page)
pp(res)
# TODO try with list of ints
