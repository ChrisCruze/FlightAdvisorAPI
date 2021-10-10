import json
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def flight_data_get():
    s3 = boto3.client('s3')
    bucket = 'flight-advisor-flights'
    key = 'flight_data_sample.csv'
    response = s3.get_object(Bucket=bucket,Key=key)
    content = response['Body']
    flight_data = content.read()
    return flight_data 

def data_response_object_generate(event,flight_data):
    queryStringParameters = event['queryStringParameters']
    origin = queryStringParameters['origin']
    destination =queryStringParameters['destination']
    airline = queryStringParameters['airline']
    return {
        'origin':origin,
        'destination':destination,
        'airline':airline,
        'flight_data':str(flight_data)
        }

def response_object_generate(data_response_object):
    return {
        'statusCode':200,
        'headers':{
            'Content-Type':'application/json'
        },
        'body':json.dumps(data_response_object)
    }

def lambda_handler_default(event,context):
    return {
        'statusCode':200,
        'body':json.dumps('hello')
    }

def lambda_handler(event, context):
    logger.info('Event: %s', event)

    flight_data = flight_data_get()
    data_response_object = data_response_object_generate(event,flight_data)
    response_object = response_object_generate(data_response_object)

    logger.info('Calculated result of %s', str(response_object))

    return response_object
