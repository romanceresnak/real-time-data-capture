from __future__ import print_function

import base64
import json
import boto3

print('Loading function')
client = boto3.client('dynamodb')

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # Amazon Kinesis data is base64-encoded so decode here
        payload = base64.b64decode(record['kinesis']['data'])
        print("Decoded payload: " + payload)
        data = json.loads(payload)
        
        # user logic for data triggered by WriteRowsEvent
        if data["type"] == "WriteRowsEvent":
            customer = data["table"]
            my_hashkey = data["row"]["values"]["customer_id"]
            my_rangekey = data["row"]["values"]["order_id"]
            my_productid = data["row"]["values"]["productid"]
            my_quantity = str( data["row"]["values"]["quantity"] )
            try:
                response = client.get_item( Key={'customerid':{'S':my_hashkey} , 'orderid':{'S':my_rangekey}} ,TableName = customer )
                if 'Item' in response:
                    item = response['Item']
                    item[data["row"]["values"]["productid"]] = {"S":my_quantity}
                    result1 = client.put_item(Item = item , TableName = customer )
                else:
                    item = { 'customerid':{'S':my_hashkey} , 'orderid':{'S':my_rangekey} , my_productid :{"S":my_quantity}  }
                    result2 = client.put_item( Item = item , TableName = customer )
            except Exception, e:
                print( 'WriteRowsEvent Exception ! :', e.message  , '==> Data:' ,data["row"]["values"]["customer_id"]  , data["row"]["values"]["order_id"] )
        
        # user logic for data triggered by UpdateRowsEvent
        if data["type"] == "UpdateRowsEvent":
            customer = data["table"]
            
        # user logic for data triggered by DeleteRowsEvent    
        if data["type"] == "DeleteRowsEvent":
            customer = data["table"]
            
            
    return 'Successfully processed {} records.'.format(len(event['Records']))