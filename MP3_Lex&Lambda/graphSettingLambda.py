import json
import boto3
import re


def lambda_handler(event, context):
    # TODO implement
    dynamodb = boto3.resource('dynamodb')
    client = boto3.client('dynamodb')
    
    City = dynamodb.Table('Cities')
    nodes = list(set(re.split('->|,', event['graph'])))
    vertices = re.split(',', event['graph'])
    
    # delete all items in the table
    flag = False
    scan = City.scan()
    while not flag:
        with City.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(
                    Key={
                        'id': each['id']
                    }
                )
            flag = True
    
    
    # graph
    c = {} # mapping from city to id
    d = {} # mapping from id to city
    id = 1
    for city in nodes:
    	if not c.get(city):
    		c[city] = id
    		d[id] = city
    		id += 1
    
    dist = []
    n = len(nodes) + 1
    
    # init distance -> all not accessable
    for i in range(n):
    	dist.append([])
    	for j in range(n):
    		dist[i].append(0x3f3f3f3f)
    
    # init given distances between cities
    for vertice in vertices:
    	v = vertice.split('->')
    	a = c[v[0]]
    	b = c[v[1]]
    	dist[a][b] = 1
    
    # floyd
    for k in range(1, n):
    	for i in range(1, n):
    		for j in range(1, n):
    			dist[i][j] = min(dist[i][k] + dist[k][j], dist[i][j])
    
    # store in dynamoDB
    dbId = 0
    for i in range(1, n):
    	for j in range(1, n):
    		if not dist[i][j] > 0x3f3f3f3f / 2:
    			fromCity = d[i]
    			toCity = d[j]
    			City.put_item(
    				Item={
    					'id': dbId,
    					'source': fromCity,
    					'destination': toCity,
    					'distance': dist[i][j]
    				}
    			)
    			print(dbId, fromCity, toCity, dist[i][j])
    			dbId += 1
    
    return {
        'statusCode': 200,
        'body': json.dumps('Store Graph Success!')
    }
