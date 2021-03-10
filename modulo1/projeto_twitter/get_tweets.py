import json
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime
import os

# Cadastrar as chaves de acesso
consumer_key = os.getenv('consumer_key')
consumer_secret = os.getenv('consumer_secret')
access_token = os.getenv('access_token')
access_token_secret = os.getenv('access_token_secret')

todays_date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

#Definir um arquivo de saída para armazenar os tweetes coletados
out = open(f"collected_tweets_{todays_date}.txt", "w")

# Implementar uma classe para conexao com o Twitter
# em python: MyListener extends StreamListner (herança)
class MyListener(StreamListener):

  def on_data(self, data):
    itemString = json.dumps(data)
    out.write(itemString + "\n")
    return True
  
  def on_error(self, status):
    print(status)

#Implementar a função MAIN

if __name__ == "__main__":
  #Instancia a classe em python
  listener = MyListener()
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)

  stream = Stream(auth, listener)
  stream.filter(track=["Trump"])