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
	
def make_user_data(f, d={}):
	d['screen_name'] = f['screen_name']
	d['followers_count'] = f['followers_count']
	d['friends_count'] = f['friends_count']
	d['listed_count'] = f['listed_count']
	d['created_days_ago'] = (datetime.utcnow().replace(tzinfo=pytz.utc) - datetime.strptime(f['created_at'], "%a %b %d %H:%M:%S %z %Y")).days
	d['favourites_count'] = f['favourites_count']
	d['time_zone'] = f['time_zone']
	d['geo_enabled'] = f['geo_enabled']
	d['verified'] = f['verified']
	d['statuses_count'] = f['statuses_count']
	d['contributors_enabled'] = f['contributors_enabled']
	d['is_translator'] = f['is_translator']
	d['is_translation_enabled'] = f['is_translation_enabled']
	d['profile_use_background_image'] = f['profile_use_background_image']
	d['has_extended_profile'] = f['has_extended_profile']
	d['default_profile'] = f['default_profile']
	d['default_profile_image'] = f['default_profile_image']
	d['following'] = f['following']
	d['live_following'] = f['live_following']
	d['follow_request_sent'] = f['follow_request_sent']
	d['notifications'] = f['notifications']
	d['muting'] = f['muting']
	d['blocking'] = f['blocking']
	d['blocked_by'] = f['blocked_by']
	d['translator_type'] = f['translator_type']
	return d
	
def get_all_followers_data(full_tweet, cursor=-1, d={}):
	resp = get_followers(full_tweet)
	count = 0
	for line in resp.iter_lines():
		followers = json.loads(line)
		if "next_cursor" in followers:
			nextCursor = followers["next_cursor"]
			d = get_all_followers_data(full_tweet, cursor=nextCursor, d=d)
			for f in followers["users"]:
				d = make_user_data(f, d)
				count += 1
	print(count)
	return d
	
def send_tweets_to_spark(http_resp, tcp_connection):
	for line in http_resp.iter_lines():
		#try:
			full_tweet = json.loads(line)
			for hashtag in full_tweet["entities"]["hashtags"]:
				if any(t in hashtag["text"].lower() for t in tracking):
					tweet_text = full_tweet['text']
					print("Tweet Text: " + tweet_text)
					print ("------------------------------------------")
					print(full_tweet["entities"]["hashtags"])
					print(full_tweet["user"]["screen_name"])
					print ("------------------------------------------")
					d = get_all_followers_data(full_tweet)
					print(d)
					#tcp_connection.send(tweet_text + '\n')
		#except:
			#e = sys.exc_info()[0]
			#print("Error: %s" % e)
        
if __name__ =="__main__":	
	TCP_IP = "localhost"
	TCP_PORT = 9009
	conn = None
	#s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	#s.bind((TCP_IP, TCP_PORT))
	#s.listen(1)
	print("Waiting for TCP connection...")
	#conn, addr = s.accept()
	print("Connected... Starting getting tweets.")
	while True:
		resp = get_tweets()
		send_tweets_to_spark(resp, conn)
