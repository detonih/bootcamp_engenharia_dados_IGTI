import json
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime

# Cadastrar as chaves de acesso
consumer_key = "Uu2CJwUA0FwqavsNxMcZ9Pf0D"
consumer_secret = "5kGsLxfDL5YhIx6qAjsVuSeuEoE8gvv57MPp7aTFhum9m5Br6x"
access_token = "1171765170399514624-3op7Za6QFkUcfNiyoGnYLks5qeejpa"
access_token_secret = "bX2J9cLbcvRbDanInG4VOWzpZpr5jQ8piz84CeDzr0XSt"

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