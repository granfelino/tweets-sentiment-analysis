from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import requests
import json
import time

def get_tweets_text(response):
    response = response["globalObjects"]["tweets"]
    tweets = []
    for item in response.values():
        tweets.append(item["full_text"])
    return tweets

def make_requests(search_term, headers, 
                  num_requests=1, sleep_time=3, api_url="https://twitter135.p.rapidapi.com/Search/"):
    querystring = {"q":search_term,"count":"20","tweet_search_mode":"live"}
    response_list = []
    
    for i in range(num_requests):
        response = requests.get(api_url, headers=headers, params=querystring)    
        if response.status_code == 200:
            response = response.json()
            tweets = get_tweets_text(response)
            for tweet in tweets:
                response_list.append(tweet)
            print(f"sent the {i} request successfully")
        else:
            raise Exception(f"Exception. Status code: {response.status_code}")
        time.sleep(sleep_time)

    return response_list



if __name__ == "__main__":

    # path to rapid_api_key which contains my rapid api key
    with open("/home/jovyan/notebooks/spark_streaming/rapid_api_key.txt") as f:
        rapid_api_key = f.readline()
        rapid_api_key = rapid_api_key.replace(' ', '')
        rapid_api_key = rapid_api_key.replace('\n', '')

    spark = SparkSession.builder.appName("stream_get").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    SEARCH_TERM = "(#tesla OR #elonmusk)"
    NUM_REQUESTS = 3
    SLEEP_TIME = 3

    HEADERS = {
        "X-RapidAPI-Key": str(rapid_api_key),
        "X-RapidAPI-Host": "twitter135.p.rapidapi.com"
    }
    response_list = make_requests(SEARCH_TERM, HEADERS, num_requests=NUM_REQUESTS, sleep_time=SLEEP_TIME)


    spark.createDataFrame(response_list, StringType()).write\
            .format("kafka")\
            .option("kafka.bootstrap.servers", "broker:9092")\
            .option("topic", "twitter")\
            .save()
    print("saved to kafka dataframe successfully")
    # program exits at this point



