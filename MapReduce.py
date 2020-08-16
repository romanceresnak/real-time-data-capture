#!/usr/bin/env python
import sys
import boto.dynamodb
 
# connection to DynamoDB
current_keys = None
conn = boto.dynamodb.connect_to_region( '<region>', aws_access_key_id='<access key id>', aws_secret_access_key='<secret access key>')
table = conn.get_table('<dynamodb table name>')
item_data = {}

# input comes from STDIN emitted by Mapper
for line in sys.stdin:
    line = line.strip()
    dickeys, items  = line.split('\t')
    products = items.split(',')
    if current_keys == dickeys:
       item_data[products[0]]=products[1]  
    else:
        if current_keys:
          try:
              mykeys = current_keys.split(',') 
              item = table.new_item(hash_key=mykeys[0],range_key=mykeys[1], attrs=item_data )
              item.put() 
          except Exception ,e:
              print 'Exception occurred! :', e.message,'==> Data:' , mykeys
        item_data = {}
        item_data[products[0]]=products[1]
        current_keys = dickeys

# put last data
if current_keys == dickeys:
   print 'Last one:' , current_keys #, item_data
   try:
       mykeys = dickeys.split(',')
       item = table.new_item(hash_key=mykeys[0] , range_key=mykeys[1], attrs=item_data )
       item.put()
   except Exception ,e:
print 'Exception occurred! :', e.message, '==> Data:' , mykeys