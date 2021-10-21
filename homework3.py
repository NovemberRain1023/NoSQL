#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install boto3


# In[2]:


import boto3


# In[37]:


s3 = boto3.resource('s3',
aws_access_key_id='AKIA2HLNT4QFRKMSBV',
aws_secret_access_key='QzFmwOf8u98rNw1mgkAPdf39euBFJs6gyB88g'
)


# In[39]:


try:
    s3.create_bucket(Bucket='arthas1023', CreateBucketConfiguration={
        'LocationConstraint': 'us-east-2'})
except Exception as e:
    print (e)


# In[40]:


bucket = s3.Bucket("arthas1023")


# In[41]:


bucket.Acl().put(ACL='public-read')


# In[42]:


pwd


# In[45]:


body = open('D:\CMU\14848\exp1', 'rb')


# In[44]:


pwd


# In[49]:


body = open('C:\code4C\\exp1.csv', 'rb')


# In[50]:


o = s3.Object('arthas1023', 'test').put(Body=body )


# In[51]:


s3.Object('arthas1023', 'test').Acl().put(ACL='public-read')


# In[52]:


dyndb = boto3.resource('dynamodb',
    region_name='us-east-2',
    aws_access_key_id='AKIA2HLNT4QFRKMSBV',
    aws_secret_access_key='QzFmwOf8u98rNw1mgkAPdf39euBFJs6gyB88g'
)


# In[53]:


try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except Exception as e:
    print (e)
    #if there is an exception, the table may already exist. if so...
    table = dyndb.Table("DataTable")


# In[54]:


table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')


# In[55]:


print(table.item_count)


# In[56]:


import csv


# In[62]:


with open('C:\code4C\experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print (item)
        body = open('C:\code4C\datafiles\\'+item[3], 'rb')
        s3.Object('arthas1023', item[3]).put(Body=body )
        md = s3.Object('arthas1023', item[3]).Acl().put(ACL='public-read')
        
        url = " https://s3-us-west-2.amazonaws.com/arthas1023/"+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
        'description' : item[4], 'date' : item[2], 'url':url}
        
        try:
            table.put_item(Item=metadata_item)
        except:
            print ("item may already be there or another failure")


# In[66]:


response = table.get_item(
    Key={
        'PartitionKey': 'experiment3',
        'RowKey': '3'
    }
)
item = response['Item']
print(item)


# In[67]:


response


# In[ ]:




