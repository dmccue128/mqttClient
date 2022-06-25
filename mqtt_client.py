#!/usr/bin/env python3

import time
from secrets import secrets
from paho.mqtt import client as mqtt_client

boat_prefix = "dt/boat/"
sensor_prefix = boat_prefix + "sensor/"
all_sensor_feeds = sensor_prefix + "#"
mqtt_client_id = "boat-client"

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT server")
        else:
            print("Failed to connect to MQTT server")

    client = mqtt_client.Client(mqtt_client_id)
    client.username_pw_set(secrets['mqtt_username'], secrets['mqtt_password'])
    client.on_connect = on_connect
    client.connect(secrets['broker'], secrets['port'])
    return client

def record_data(file_label, payload):
    f = open(f'{file_label}_data.csv', "a")
    f.write(f'{time.strftime("%Y-%m-%d,%H:%M:%S", time.localtime())},{payload}' )
    f.close()

def process_message(client, userdata, msg):
    topic = msg.topic.removeprefix(sensor_prefix)
    label = topic.replace('/', '_')
    record_data(label, msg.payload.decode())

def run():
    client = connect_mqtt()
    client.subscribe(all_sensor_feeds)
    client.on_message = process_message
    client.loop_forever()

if __name__ == "__main__":
    run()
