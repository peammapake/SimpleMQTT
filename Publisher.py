from socket import *
import socket
import sys

# Publisher program is part of MQTT application 
# its role is to connect to broker and send 
# topic with message to the broker 

SERV_PORT = 5001        # Default port for broker
MAX_BUF = 2048

sub = 'PUB'
# Loop if the program unable to connect to the given ip
while True:
    ip = input('Please enter broker ip: ')
    addr = (ip, SERV_PORT)
    s = socket.socket(AF_INET,SOCK_STREAM)
    try:
        s.connect(addr)
    except socket.error:
        print('Error: Unable to connect to Broker')
        continue
    break

s.send(sub.encode('utf-8'))             # Nofify broker that this program is a Publisher

print('MQTT publisher started..')
print('Please Enter:  "topic_name" "data"')

# Loop to continually let the user send topic and data to the broker
while True:
    try:
        txtout = input("Publish: ")
        # if user insert quit, it will notify broker that this publisher is going to disconnect
        if txtout == 'quit':
            s.send(txtout.encode('utf-8'))
            s.close
            break
        # Try split the string into topic and data if failed, notify user and retry
        try:
            topic, data = txtout.split()
        except ValueError:
            print('-- Input incorrectly filled')
            continue
        sys.stdout.flush()
        s.send(txtout.encode('utf-8'))
    # If found keyboard interrupt, send quit to notify broker and exit program
    except (KeyboardInterrupt,SystemExit,socket.error):
        try:
            s.send('quit'.encode('utf-8'))
            s.close()
            sys.exit(0)
        except SystemExit:
            s.close()
            os._exit(0)


    

