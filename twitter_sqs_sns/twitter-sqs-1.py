
import boto3
from twython import Twython
import json
import time

#Access_Keys to be obtained from Twitter Application
#Used to make Twitter API request

TWITTER_ACCESS_TOKEN = ''
TWITTER_ACCESS_TOKEN_SECRET = ''

#Consumer keys to access Twitter Application 

TWITTER_APP_KEY = ''
TWITTER_APP_KEY_SECRET = ''

SQS_QUEUE_NAME='Twittrends'

#Authenticating Credentials 

twitterauth = Twython(app_key=TWITTER_APP_KEY,
            app_secret=TWITTER_APP_KEY_SECRET,
            oauth_token=TWITTER_ACCESS_TOKEN,
            oauth_token_secret=TWITTER_ACCESS_TOKEN_SECRET)

#Initializing SQS

sqs = boto3.resource( 'sqs', aws_access_key_id="", aws_secret_access_key="",region_name='us-east-1')

queue = sqs.get_queue_by_name(QueueName=SQS_QUEUE_NAME)

print (queue.url)


def get_tweets(keyword):
    print ("Pulling Tweets")
    search = twitterauth.search(q=keyword,count=100)
    tweets = []
    tweets = search['statuses']
    for tweet in tweets:

        if tweet['geo'] != None:
            
            print (tweet['user']['lang'])
            if tweet['user']['lang']=='en':
                text = tweet['text'].lower().encode('ascii','ignore').decode('ascii')
                index = tweet['id']
                coordinates = tweet['geo']['coordinates']
                
                message={
                'id':index,
                'text':text,
                'coordinates':coordinates,
                'sentiment':''
                }
                
                temp=json.dumps(message)
                print (temp)
                response = queue.send_message(MessageBody=temp)




def twittmap():
    try:
        for i in range(1,20):
            get_tweets('movies')
            time.sleep(5)
            get_tweets('technology')
            time.sleep(5)
            get_tweets('sports')
            time.sleep(5)
            get_tweets('life')
            time.sleep(5)
            get_tweets('news')
            time.sleep(5)
            get_tweets('travel')
            time.sleep(5)
            get_tweets('health')
            time.sleep(5)
            get_tweets('awesome')
            time.sleep(5)
            get_tweets('energy')
            time.sleep(5)
            get_tweets('music')
            time.sleep(5)
    except Exception as e:
        print(str(e)) 
        
        #pass
        return

if __name__ == '__main__':
    while True:
        twittmap()
        
    
