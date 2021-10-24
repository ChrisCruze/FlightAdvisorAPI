import json
import boto3
import logging 
import pandas as pd
import glob

logger = logging.getLogger()
logger.setLevel(logging.INFO)


#https://stackoverflow.com/questions/43355074/read-a-csv-file-from-aws-s3-using-boto-and-pandas
def flight_data_df_from_response(response):
    initial_df = pd.read_csv(response['Body'])
    return initial_df

def flight_data_get(bucket = 'flight-advisor-flights',key = 'flight_data_sample.csv'):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket,Key=key)
    return flight_data_df_from_response(response)
    #content = response['Body']
    #flight_data = content.read()
    #return flight_data 

def airports_pull():
    df = flight_data_get(key = 'L_AIRPORT.csv')
    return df 

def airlines_pull():
    df = flight_data_get(key = 'L_UNIQUE_CARRIERS.csv')
    return df 

def data_pull():
    df = flight_data_get(key = 'flight_data.csv')
    return df 

def airports_unique_df_generate(df):
    origin_array = df['ORIGIN'].unique()
    dest_array = df['DEST'].unique()
    combined_array = list(origin_array) + list(dest_array)
    combined_array = list(set(combined_array))
    origin_dest_df = pd.DataFrame({'Code':combined_array})
    return origin_dest_df

def airports_label_values_generate(df):
    origin_dest_df = airports_unique_df_generate(df)
    airports_df = airports_pull()
    combined_df = origin_dest_df.set_index('Code').join(airports_df.set_index('Code'),on='Code')
    tups = combined_df.reset_index().to_numpy()
    return [{'value':value,'label': str(label) + " (" + str(value) + ") "} for value,label in tups]


def airlines_unique_df_generate(df):
    origin_array = df['MKT_UNIQUE_CARRIER'].unique()
    airlines_df = pd.DataFrame({'Code':origin_array})
    return airlines_df

def airlines_label_values_generate(df):
    airlines_unique_df = airlines_unique_df_generate(df)
    airlines_dictionary_df = airlines_pull()
    combined_df = airlines_unique_df.set_index('Code').join(airlines_dictionary_df.set_index('Code'),on='Code')
    tups = combined_df.reset_index().to_numpy()
    return [{'value':value,'label': label + " (" + value + ") "} for value,label in tups]

def origin_dest_airline_filter(df,origin,dest,airline):
    df_copy = df.copy()
    #filt = (df_copy['MKT_CARRIER'] == airline) & (df_copy['ORIGIN'] == origin) & (df_copy['DEST'] == dest)
    filt = (df_copy['MKT_UNIQUE_CARRIER'] == airline) & (df_copy['ORIGIN'] == origin) & (df_copy['DEST'] == dest)
    filtered_df = df_copy[filt]
    return filtered_df

def flight_delay_filter(df,delay_threshold=20):
    df_copy = df.copy()
    filt = (df_copy['ARR_DELAY'] >= delay_threshold) 
    filtered_df = df_copy[filt]
    return filtered_df

def delay_frequency_calculate_from_data(df,origin,dest,airline):
    logger.info('origin: %s', str(origin))
    logger.info('dest: %s', str(dest))
    logger.info('airline: %s', str(airline))
    filtered_df = origin_dest_airline_filter(df,origin,dest,airline)
    flight_delays_df = flight_delay_filter(filtered_df)
    flights_total_count = len(filtered_df.index)
    flight_delay_count = len(flight_delays_df.index)
    logger.info('flight_delay_count: %s', str(flight_delay_count))
    logger.info('flights_total_count: %s', str(flights_total_count))
    

    if flights_total_count == 0:
        flight_delay_percentage = 0 
    else:
        flight_delay_percentage =  flight_delay_count/flights_total_count

    delay_frequency_dict = {
        'flight_delay_percentage':flight_delay_percentage,
        'flight_total_count':flights_total_count,
        'flight_delay_count':flight_delay_count,
        'origin':origin,
        'destination':dest,
        'airline':airline
    }
    return delay_frequency_dict


def delay_frequency(origin,dest,airline):
    df = data_pull()
    return delay_frequency_calculate_from_data(df,origin,dest,airline)





def data_response_object_generate(event,flight_data):
    queryStringParameters = event['queryStringParameters']
    # origin = queryStringParameters['origin']
    # destination =queryStringParameters['destination']
    # airline = queryStringParameters['airline']
    origin = queryStringParameters['origin']
    destination =queryStringParameters['destination']
    airline = queryStringParameters['airline']
    delay_frequency_dict = delay_frequency(origin,destination,airline)
    return delay_frequency_dict
    # return {
    #     'origin':origin,
    #     'destination':destination,
    #     'airline':airline,
    #     'delay_frequency':delay_frequency
    #     # 'flight_data':str(flight_data)
    #     }

def response_object_generate(data_response_object):
    return {
        'statusCode':200,
        'headers':{
            #'Content-Type':'application/json',
            #'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        ,
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
