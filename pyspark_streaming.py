import tweepy
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

auth = tweepy.OAuthHandler( "5GfAOVFbva56SfFRrxPwu6S7K", "FSrHlgAKFTRR3I7JTBeFCouweQpoqrPCHCZBAdKKYFUCrthbdy")
auth.set_access_token( "912719203689422854-4GmOCIIINULb4Dpwsefr8IciUKpSk4b",  "44bTrObNAjML1FAGLJDjUOQkYQYgvzjUaDeaZNF5jtn39")

api = tweepy.API(auth)
sc=SparkContext("local","pyspark_streaming")
ssc=StreamingContext(sc,1)
lines=ssc.socketTextStream("https://mobile.twitter.com/narendramodi",9999)
lines.pprint()

ssc.start()
ssc.awaitTermination()

