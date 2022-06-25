#!/usr/bin/env python3

import time
from secrets import secrets
from paho.mqtt import client as mqtt_client

boat_prefix = "dt/boat/"
sensor_prefix = boat_prefix + "sensor/"
battery_prefix = sensor_prefix + "battery/"
battery_voltage_feed = battery_prefix + "voltage"
battery_current_feed = battery_prefix + "current"

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
    client.username_pw_set(secrets['mqtt_username'], secrets['mqtt_password'])
    client.on_connect = on_connect
    client.connect(secrets['broker'], secrets['port'])
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
    subscribe(client, battery_voltage_feed, record_voltage)
    subscribe(client, battery_current_feed, record_current)
    client.on_message = dispatcher
    client.loop_forever()

if __name__ == "__main__":
    run()
