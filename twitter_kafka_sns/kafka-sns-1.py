
from multiprocessing import Pool, TimeoutError, Lock
import threading
import time
import multiprocessing
from watson_developer_cloud import AlchemyLanguageV1
import boto3
from kafka import KafkaConsumer
import json

KAFKA_HOST = 'localhost:9092'
TOPIC = 'test'
lock = Lock()
alchemy_api_key = '967b10634e30e3d4d868feb59d70fbded7432c45' 
alchemy_language = AlchemyLanguageV1(api_key=alchemy_api_key)


def getKafka():
    lock.acquire()

    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[KAFKA_HOST],consumer_timeout_ms=10000)
    for message in consumer:

        tweet = json.loads(message.value)
        if tweet is not None :
            #getting sentiment
            
            result = json.loads(json.dumps( alchemy_language.sentiment( text = tweet['text']), indent = 2))
            docSentiment =  result['docSentiment']
            sentiment = docSentiment['type']
            tweet['sentiment'] = sentiment
            tweet = json.dumps(tweet)
            print (tweet)
    
    
            client = boto3.client('sns', aws_access_key_id="AKIAI37GWBF2JTQEFKDQ", aws_secret_access_key="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",region_name='us-east-1')
            response = client.publish(
                TargetArn='arn:aws:sns:us-east-1:743728513790:Twittrends-sns-3',
                Message=json.dumps({'default': tweet}),
                MessageStructure='json'
            )

    lock.release()
	    
if __name__ == '__main__':
    

    pool = Pool(processes=4)              
    

    while 1:
    	multiple_results = [pool.apply_async(getKafka, ()) for i in range(4)]
    	print ([res.get(timeout=100) for res in multiple_results])
    	time.sleep(2)
    
