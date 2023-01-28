import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    # TODO implement
    dynamodb = boto3.resource('dynamodb')
    client = boto3.client('dynamodb')
    
    City = dynamodb.Table('Cities')
    
    source = event['currentIntent']['slots']['Source']
    destination = event['currentIntent']['slots']['Destination']
    
    
    target = City.scan(
		FilterExpression=Attr('source').eq(source) & Attr('destination').eq(destination),
	)
    
    distance = target['Items'][0]['distance']
    
    response = {
    	"dialogAction": {
    		"type": "Close",
    		"fulfillmentState": "Fulfilled",
    		"message": {
    			"contentType": "PlainText",
    			"content": int(distance)
    		}
    	}
    }
    
    return response
