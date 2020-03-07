from socket import *
import socket
import sys
import time

# Subscriber program is part of MQTT application 
# its role is to connect to broker and listen on 
# the message from the publisher it subscribed to

MAX_BUF = 2048
SERV_PORT = 5001    #Default port for the program to connect to

sub = 'SUB'

print('MQTT subscriber started..')
# Loop if user's input is incorrect
while True:
    inputTxt = input('Subscribe: ')
    # Try split the input into two string if fail then user's input is incorrect
    try:                                       
        ip, topic = inputTxt.split()
    except ValueError:
        print('Error: Wrong input format')
        continue
    addr = (ip, SERV_PORT)
    s = socket.socket(AF_INET,SOCK_STREAM)
    try:
        # Try connect to the set IP. if unable to connect, let user try again
        s.connect(addr)
    except socket.error:
        print('Error: Unable to connect to Broker')
        continue
    break
    

sys.stdout.flush()
s.send(sub.encode('utf-8'))         # Notify broker that this program is a subscriber
time.sleep(0.2)                     # Give the first message time to send message
s.send(topic.encode('utf-8'))       # Notify the topic this Program subscribe to

while True:
    # The program continuously receive message from broker
    try:
        txtin = s.recv(2048).decode('utf-8')
        print('Subscribe: ' + txtin)
    # If receive keyboard interrupt or system exit, break connection and close program
    except (KeyboardInterrupt,SystemExit,socket.error):
        try:
            s.close()
            sys.exit(0)
        except SystemExit:
            s.close()
            os._exit(0)


    
