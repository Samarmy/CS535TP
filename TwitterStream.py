import socket
import sys
import requests
import requests_oauthlib
import json
from datetime import datetime
import pytz


ACCESS_TOKEN = '1103080779268026368-pcCTQueex8vi78QeHnhAPklpmGL54e'
ACCESS_SECRET = 'NbrJAAXHRD3v3fSccubvGdQMqykwFOKo1ezQjC5GgkHxa'
CONSUMER_KEY = '3MdU37FpmK5GIJIF7CwvmHwbu'
CONSUMER_SECRET = 'PnV5RTi8uHy4QH1rFFlWG21ubOpDOWhncy8ovqD6T32axshrkD'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

tracking = ["2020census","marvel","politics"]

def get_tweets():
	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
	query_data = [('language', 'en'),('track',",".join(tracking))]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
	response = requests.get(query_url, auth=my_auth, stream=True)
	return response
	
def get_followers(ft, cursor=-1):
	url = 'https://api.twitter.com/1.1/followers/list.json'
	query_data = [('screen_name', ft["user"]["screen_name"]), ("cursor", cursor)]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
	response = requests.get(query_url, auth=my_auth, stream=True)
	return response
	
def make_user_data(f, d=""):
	d += str(f['screen_name']) + ","
	d += str(f['followers_count']) + ","
	d += str(f['friends_count']) + ","
	d += str(f['listed_count']) + ","
	d += str(datetime.strptime(f['created_at'], "%a %b %d %H:%M:%S %z %Y")) + ","
	d += str(f['favourites_count']) + ","
	d += str(f['time_zone']) + ","
	d += str(f['geo_enabled']) + ","
	d += str(f['verified']) + ","
	d += str(f['statuses_count']) + ","
	d += str(f['contributors_enabled']) + ","
	d += str(f['is_translator']) + ","
	d += str(f['profile_use_background_image']) + ","
	d += str(f['default_profile']) + ","
	d += str(f['default_profile_image']) + ","
	d += str(f['following']) + ","
	d += str(f['follow_request_sent']) + ","
	d += str(f['notifications']) + ","
	d += str(f['translator_type']) + ":"
	return d
	
def get_all_followers_data(full_tweet, cursor=-1, d="", u=""):
	resp = get_followers(full_tweet)
	count = 0
	for line in resp.iter_lines():
		followers = json.loads(line)
		if "next_cursor" in followers:
			for f in followers["users"]:
				d = make_user_data(f, d)
				u += full_tweet["user"]["screen_name"] + "," + f["screen_name"] + ":"
				count += 1
			nextCursor = followers["next_cursor"]
			d, u = get_all_followers_data(full_tweet, cursor=nextCursor, d=d, u=u)
	print(count)
	return d, u
	
def get_all_hashtag_data(full_tweet):
	d = ","
	d += str(full_tweet["quote_count"]) + ","
	d += str(full_tweet["reply_count"]) + ","
	d += str(full_tweet["retweet_count"]) + ","
	d += str(full_tweet["favorite_count"]) + ","
	d += str(full_tweet["favorited"]) + ","
	d += str(full_tweet["retweeted"]) + ","
	d += str(full_tweet["filter_level"]) + ","
	return d
	
def send_tweets_to_spark(http_resp, tcp_connection):
	for line in http_resp.iter_lines():
		try:
			full_tweet = json.loads(line)
			for hashtag in full_tweet["entities"]["hashtags"]:
				if any(t == hashtag["text"].lower() for t in tracking):
					tweet_text = full_tweet['text']
					print(full_tweet.keys())
					print("Tweet Text: " + tweet_text)
					print ("------------------------------------------")
					print(full_tweet["entities"]["hashtags"])
					print(full_tweet["user"]["screen_name"])
					print ("------------------------------------------")
					d, u = get_all_followers_data(full_tweet)
					d = make_user_data(full_tweet["user"], d)
					hd = get_all_hashtag_data(full_tweet)
					print(full_tweet["user"]["screen_name"] + "," + hashtag["text"].lower() + hd + '\n')
					tcp_connection[0].send((u + '\n').encode('utf-8'))
					tcp_connection[1].send((full_tweet["user"]["screen_name"] + "," + hashtag["text"].lower() + hd + '\n').encode('utf-8'))
					tcp_connection[2].send((d + '\n').encode('utf-8'))
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)
        
if __name__ =="__main__":	
	TCP_IP = "tallahassee"
	TCP_PORTS = [11711, 11712, 11713]
	conn = [None, None, None]
	for i, port in enumerate(TCP_PORTS): 
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.bind((TCP_IP, port))
		s.listen(1)
		print("Waiting for TCP connection...")
		c, addr = s.accept()
		conn[i] = c
	print("Connected... Starting getting tweets.")
	while True:
		resp = get_tweets()
		send_tweets_to_spark(resp, conn)
