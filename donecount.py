#!/usr/bin/env python3
"""Show we can do DB-enforced page counting and know we're done easily.

Do an update which increments pagecount and inserts the currently done page
into pagesdone list atomically, only if the page number doesn’t already exist.

It will the new page count (and page list), for free, as a consistent read IIRC.

If the count is equal to the total_pages, then the lambda can know we’re done,
and trigger a consolidation lambda.

This should be very fast, and enforce consistency.

Since we're using ADD, we don't even need the item to exist, or if it does,
have values in count and done -- it initializes them to 0 and set()
respectively.

Will the data become too large and cause a RCU/WCU slowdown if we have docs
with 1000 pages? 30,000 pages? Let’s say we store pages as a string, each of 5
bytes. 30000 pages * 5 bytes = 150KB. This is within the limit of DDB item size
but 150KB/4KB/RCU = 37.5 RCU.

For 1000 pages in done (count is off because of initial test data):
  page=999 count=1000 RCU={'CapacityUnits': 3.0}
"""

# in AWS_PROFILE=wp-dev
# pk=doc1, sk=na : count=Number(42), done=NumberSet{1,3,5,7,11,13,17,19}
# TODO: make total_pages an attribute of this DDB item

from pprint import pprint as pp
from random import randint

import boto3
from botocore.exceptions import ClientError

DDB = 'cshenton-schema'

dbr = boto3.resource('dynamodb')
dbt = dbr.Table(DDB)

# Add to done (SN set of numbers) and increment count

for page in range(1000):
    try:
        res = dbt.update_item(
            Key={'pk': 'doc3', 'sk': 'na'},
            ReturnConsumedCapacity='INDEXES', ReturnValues='ALL_NEW',
            ExpressionAttributeNames={
                '#count': 'count',
                '#done': 'done',
            },
            ExpressionAttributeValues={
                ':1': 1,
                ':page': page,
                ':pagelist': set([page]),
            },
            ConditionExpression="(NOT contains(done, :page))",
            UpdateExpression="ADD #done :pagelist, #count :1",
        )
        print(f'rand int page={page} count={res["Attributes"]["count"]}'
              f'RCU={res["ConsumedCapacity"]["Table"]}')
    except ClientError as err:
        if err.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print('Already got page=%s (%s)' % (page, err))
        else:
            raise

exit(0)

###############################################################################
# Show the LIST (of random types) works:
page = randint(0, 1000)
page_str = str(page)
res = dbt.update_item(
    Key={'pk': 'doc2', 'sk': 'na'},
    ReturnConsumedCapacity='INDEXES', ReturnValues='ALL_NEW',
    ExpressionAttributeNames={
        '#count': 'count',
        '#done': 'done',
    },
    ExpressionAttributeValues={
        ':1': 1,
        ':page': str(page),
        ':pagelist': [str(page)],
    },
    ConditionExpression="(NOT contains(done, :page))",
    UpdateExpression="SET #count = #count + :1, done = list_append(#done, :pagelist)",
)
print('rand str page=%s' % page)
pp(res)
