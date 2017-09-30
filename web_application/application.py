
from flask import Flask, render_template, request
from elasticsearch import Elasticsearch
import certifi
import json
import requests
import sys

application = Flask(__name__)

#Keywords
keyword_dict = {'keyword 1':'movies','keyword 2':'technology','keyword 3':'sports','keyword 4':'life','keyword 5':'news','keyword 6':'travel','keyword 7':'health','keyword 8':'awesome','keyword 9':'energy','keyword 10':'music','Nothing here':'no','':'no'}
geo_dict = {'d1':10,'d2':50,'d3':100,'d4':200,'d5':500,'d6':1000,'d7':5000,'Nothing here':0,'': 0}

elastic_search_host = "search-twittrend-es-yae67nigk6vybgmn2pnmulohle.us-east-1.es.amazonaws.com"

#Define ElasticSearch credentials
es = Elasticsearch(hosts = [{"host" : elastic_search_host,
                              "port" : 443}],
                              use_ssl='True')

#Handle POST Requests
@application.route ('/',methods = ['POST','GET'])
def update_map2():
    
    r = ""
    center_lat = 0.0
    center_long = 0.00
    dist = "Nothing here"
    tweets = []
    key = "Nothing here"
    if request.method == 'POST':
        
        try:
            
            js =request.data.decode("utf-8")
            js = json.loads(js)
            if js["Type"]=="SubscriptionConfirmation":
            
                print("Subscription Request")
                r = str(requests.get(js['SubscribeURL']))
            
            elif js["Message"]:
                print("Message Recieved")
                message = json.loads(js["Message"])
                es.index(index='final-tweet-index',doc_type='twitter',body = message)
                
            print(js,file=sys.stderr)
            
        except:
            
            if request.form['keyword'] != None:
                key = request.form['keyword']
                tweets = gettweets(keyword_dict[key])
                
            else:
                print("exception")
            
            pass

    #Update HTML webpage
    return render_template("index.html",lat = center_lat, long = center_long, key = keyword_dict[key], dist = geo_dict[dist], tweets = tweets)
    
#Get tweets from elasticsearch
def gettweets(keyword):
    
    tweets = []
    
    if keyword == "no":
        keyword = ""

    #body_content = {"query":{"match":{"_all":keyword}}}
    body_content={"query": {"match": { "text": { "query": keyword, "operator": "or" } } } }
    #Get relevant tweet stream
    stream = es.search(index = "final-tweet-index", doc_type = "twitter", body = body_content, size = 10000)
        
    tweets = stream["hits"]["hits"]
    print(tweets)
    return tweets   

if __name__ == "__main__":
    
    application.run(host='0.0.0.0', debug = True)
