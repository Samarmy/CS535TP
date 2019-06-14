import socket
import sys
import requests
import requests_oauthlib
import json
from datetime import datetime
import pytz

my_auths = []
#Kevin
ACCESS_TOKEN = '1103080779268026368-pcCTQueex8vi78QeHnhAPklpmGL54e'
ACCESS_SECRET = 'NbrJAAXHRD3v3fSccubvGdQMqykwFOKo1ezQjC5GgkHxa'
CONSUMER_KEY = '3MdU37FpmK5GIJIF7CwvmHwbu'
CONSUMER_SECRET = 'PnV5RTi8uHy4QH1rFFlWG21ubOpDOWhncy8ovqD6T32axshrkD'
my_auths.append(requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET))
#David
ACCESS_TOKEN = '1044286223991267328-SyR5Bz0hvGPgsbba5Jhj9MuCpjdXar'
ACCESS_SECRET = 'mWQa421ME1DB1T5a8Ka0b1oxs8D6d9R5h8cTSBUqePJRi'
CONSUMER_KEY = '2d9PhgTD6YfmT8iQWheeaByd3'
CONSUMER_SECRET = 'ZC6aamzKCx21ZODCUFrkFLNNQBlmptsL0x66OBtQgkRuMGX0dg'
my_auths.append(requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET))
#Sam
ACCESS_TOKEN = '1108447348210962432-pgAB79vs7jPoSubP6vxaZ8ErJWGW00'
ACCESS_SECRET = '3D3PdlXpA9YV5YYRDDiHyFnuJuRrJAy8eNXHWV1Y3HtdO'
CONSUMER_KEY = 'ZVrRMyXUstvQGjdeZSjMvrhq0'
CONSUMER_SECRET = 'JDXQ4r5S0ZbqhBQvKUBnH6EMLw5lNtBo9CPCjdR1GhjU1ZSvLT'
my_auths.append(requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET))
#Sam2
ACCESS_TOKEN = '1138886152428146688-akWhdxr39ZE3jOqiDhDpiqNANbIPKh'
ACCESS_SECRET = '2EuwY7KeQP6FoX9MZeEDQRQ8z4TkQWfFzidDGILajfopa'
CONSUMER_KEY = 'A6BBTqLLxfEICxHL4vqGuaZvE'
CONSUMER_SECRET = 'R8GvEWiLyi7HpHgHNVfoSB7JZ4ahz4s9EEHsimnbUEVKc4p65a'
my_auths.append(requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET))

with open('hashtags.txt') as f:
    tracking = f.read().splitlines()
print(tracking)

def get_tweets(auth):
	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
	query_data = [('language', 'en'),('track',",".join(tracking))]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
	response = requests.get(query_url, auth=auth, stream=True)
	return response

def get_followers(auth, ft, cursor=-1):
	url = 'https://api.twitter.com/1.1/followers/list.json'
	query_data = [('screen_name', ft["user"]["screen_name"]), ("cursor", cursor)]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
	response = requests.get(query_url, auth=auth, stream=True)
	return response

def make_user_data(f, d=""):
	d += str(f['screen_name']) + ","
	d += str(f['followers_count']) + ","
	d += str(f['friends_count']) + ","
	d += str(f['listed_count']) + ","
	d += str(datetime.strptime(f['created_at'], "%a %b %d %H:%M:%S %z %Y").timestamp() * 1000) + ","
	d += str(f['favourites_count']) + ","
	d += str(f['verified']) + ","
	d += str(f['statuses_count']) + ","
	d += str(f['contributors_enabled']) + ","
	d += str(f['default_profile']) + ","
	d += str(f['default_profile_image']) + ","
	d += str(f['location']) + ":"
	return d

def get_all_followers_data(auths, full_tweet, cursor=-1, d="", u=""):
	i = 0
	resp = get_followers(auths[i], full_tweet)
	while resp.status_code == 429:
		i += 1
		if i == len(auths):
			break
		resp = get_followers(auths[i], full_tweet)

	count = 0
	for line in resp.iter_lines():
		followers = json.loads(line)
		if "next_cursor" in followers:
			for f in followers["users"]:
				d = make_user_data(f, d)
				u += full_tweet["user"]["screen_name"] + "," + f["screen_name"] + ":"
				count += 1
			nextCursor = followers["next_cursor"]
			d, u = get_all_followers_data(auths, full_tweet, cursor=nextCursor, d=d, u=u)
	print(count)
	return d, u

def get_all_hashtag_data(full_tweet):
	d = ","
	d += str(full_tweet["timestamp_ms"]) + ","
	d += str(full_tweet["quote_count"]) + ","
	d += str(full_tweet["reply_count"]) + ","
	d += str(full_tweet["retweet_count"]) + ","
	d += str(full_tweet["favorite_count"]) + ","
	d += str(full_tweet["favorited"]) + ","
	d += str(full_tweet["retweeted"]) + ","
	d += str(full_tweet["filter_level"]) + ","
	d += str(full_tweet['text']) + ","
	return d

def send_tweets_to_spark(auths, http_resp, tcp_connection):
	for line in http_resp.iter_lines():
		try:
			full_tweet = json.loads(line)
			if "limit" in full_tweet:
				continue
			for hashtag in full_tweet["entities"]["hashtags"]:
				if any(t == hashtag["text"].lower() for t in tracking):
					tweet_text = full_tweet['text']
					print("Tweet Text: " + tweet_text)
					print ("------------------------------------------")
					print(full_tweet["entities"]["hashtags"])
					print(full_tweet["user"]["screen_name"])
					print ("------------------------------------------")
					d, u = get_all_followers_data(auths, full_tweet)
					d = make_user_data(full_tweet["user"], d)
					hd = get_all_hashtag_data(full_tweet)
					tcp_connection[0].send((u + '\n').encode('utf-8'))
					tcp_connection[1].send((full_tweet["user"]["screen_name"] + "," + hashtag["text"].lower() + hd + '\n').encode('utf-8'))
					tcp_connection[2].send((d + '\n').encode('utf-8'))
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)

if __name__ =="__main__":
	TCP_IP = "funkwerks"
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
		resp = get_tweets(my_auths[0])
		send_tweets_to_spark(my_auths, resp, conn)
