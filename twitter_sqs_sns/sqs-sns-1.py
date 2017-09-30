
import time
import boto3
import json
from watson_developer_cloud import AlchemyLanguageV1
import os
import multiprocessing
from multiprocessing import Pool, TimeoutError

# Initializing SQS

sqs = boto3.resource( 'sqs', aws_access_key_id="AKIAI37GWBF2JTQEFKDQ", aws_secret_access_key="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",region_name='us-east-1')


SQS_QUEUE_NAME='Twittrends'

# Initializing Alchemy

alchemy_api_key = '967b10634e30e3d4d868feb59d70fbded7432c45'

alchemy_language = AlchemyLanguageV1(api_key=alchemy_api_key)


def getSQS():
    queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)
    for message in queue.receive_messages():
        tweet = json.loads(message.body)
        if tweet is not None :
            
            #Getting sentiment using Alchemy API

            result = json.loads(json.dumps( alchemy_language.sentiment( text = tweet['text']), indent = 2))
            docSentiment =  result['docSentiment']
            sentiment = docSentiment['type']
            tweet['sentiment'] = sentiment
            tweet = json.dumps(tweet)
            print (tweet)

            #Pushing data to amazon sns

            
            client = boto3.client('sns', aws_access_key_id="AKIAI37GWBF2JTQEFKDQ", aws_secret_access_key="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",region_name='us-east-1')
            response = client.publish(
                TargetArn= 'arn:aws:sns:us-east-1:743728513790:Twittrends-sns',
                Message=json.dumps({'default': tweet}),
                MessageStructure='json'
            )

            message.delete()

if __name__ == '__main__':
    pool = Pool(processes=4)
    while 1:

    	multiple_results = [pool.apply_async(getSQS, ()) for i in range(4)]
    	time.sleep(2)
