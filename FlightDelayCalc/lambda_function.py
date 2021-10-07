import json

def parameter_attain(event,name):
    return event['queryStringParameters'][name]

def data_response_object_generate(event):
    origin = parameter_attain(event,'origin')
    destination = parameter_attain(event,'destination')
    airline = parameter_attain(event,'airline')
    return {
        'origin':origin,
        'destination':destination,
        'airline':airline
        }

def response_object_generate(data_response_object):
    response_object = {}
    response_object['statusCode'] = 200
    response_object['headers']['Content-Type'] = 'application/json'
    response_object['body'] = json.dumps(data_response_object)
    return response_object 

def lambda_handler(event, context):
    data_response_object = data_response_object_generate(event)
    response_object = response_object_generate(data_response_object)
    return response_object
