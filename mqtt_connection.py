
import time
from datetime import datetime

from paho.mqtt import client as mqtt_client
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


# Use the application default credentials
cred = credentials.Certificate('./energy-coach-firebase-adminsdk-wxvhm-d5099bd43d.json')
firebase_admin.initialize_app(cred, 
{
  'projectId': 'energy-coach',
})

db = firestore.client()

broker = '192.168.100.47'
port = 1883
topic = "shellies/shellyem-B9E151/emeter/+/power"
# generate client ID with pub prefix randomly
client_id = 'shellyem-B9E151ggg'
username = 'admin'
password = 'public'


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        total_power= msg.payload.decode()
        valor1=0
        valor0=0
        #print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        
        valor1=msg.payload.decode()
            
        sum=float(valor1)*2
        print(sum)
        dtm=datetime.now()
        data={
            u'datetime':dtm,
            u'consumption':sum,

        }
        db.collection(u'shellyData').add(data)
        time.sleep(300)

    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


run()





