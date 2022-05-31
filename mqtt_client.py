#!/usr/bin/env python3

import time
import secrets
from paho.mqtt import client as mqtt_client

voltage_feed = "dt/boat/sensor/battery/voltage"
current_feed = "dt/boat/sensor/battery/current"
mqtt_client_id = "boat-client"

# create a dispatch table for subscriptions
dispatch = {}

def record_data(value, file_label):
    f = open(f'{file_label}_data.csv', "a")
    f.write(f'{time.strftime("%Y-%m-%d,%H:%M:%S", time.gmtime())},{value}' )
    f.close()

def record_voltage(client, userdata, msg):
    record_data(msg.payload.decode(),'voltage')

def record_current(client, userdata, msg):
    record_data(msg.payload.decode(),'current')

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT server")
        else:
            print("Failed to connect to MQTT server")

    client = mqtt_client.Client(mqtt_client_id)
    client.username_pw_set(secrets[mqtt_username], secrets[mqtt_password])
    client.on_connect = on_connect
    client.connect(secrets[broker], secrets[port])
    return client

def publish(topic, message):
    result = client.publish(topic, message)
    status = result[0]
    if status == 0:
        print(f'Published {message}')
    else:
        print(f'Failed to publish {message}')

def subscribe(client: mqtt_client, topic, on_recv):
    client.subscribe(topic)
    dispatch[topic] = on_recv

def dispatcher(client, userdata, msg):
    if msg.topic in dispatch:
        dispatch[msg.topic](client, userdata, msg)
    else:
        print(f'No handler for topic: {topic}')

def run():
    client = connect_mqtt()
    subscribe(client, voltage_feed, record_voltage)
    subscribe(client, current_feed, record_current)
    client.on_message = dispatcher
    client.loop_forever()

if __name__ == "__main__":
    run()
