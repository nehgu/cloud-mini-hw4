import tweepy,json
from tweepy.streaming import StreamListener
from kafka import KafkaProducer

class MyStreamListener(StreamListener):
	
	def on_data(self, data):
		data = json.loads(data)
		selected_data = {}

		if "coordinates" in data.keys() and data["coordinates"] is not None:
	
			selected_data["coordinates"]=data["coordinates"]["coordinates"]
			selected_data["text"]=data["text"]
			selected_data["lang"]=data["lang"]
			selected_data["tid"]=data["id"]
			selected_data["uname"]=data["user"]["name"]
			selected_data["epoch"]=data["timestamp_ms"]

			print ("sent  : "+  str(selected_data["tid"])+" , " +selected_data["text"])
			
			producer.send('tweets', json.dumps(selected_data).encode('utf-8'))
		 
	def on_error(self, status):
		print (status)

def readkeys():
	keys =[]
	lines = open("keys.txt").readlines()
	for line in lines:
		line_sp = line.split(":")
		keys.append(line_sp[1][:-1])
	return keys

if __name__ == '__main__':

	keys = readkeys()

	auth = tweepy.OAuthHandler(keys[0], keys[1])
	auth.set_access_token(keys[2], keys[3])

	producer = KafkaProducer(bootstrap_servers='localhost:9092')

	myStream = tweepy.Stream(auth = auth, listener=MyStreamListener())
	myStream.filter(track=["want","trump","tokyo","job","wind","winter","love","halloween"], stall_warnings=True)

