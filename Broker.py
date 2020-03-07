from socket import *
import socket
from threading import Thread,Event
import time
import os,sys
import traceback

# Broker is part of MQTT application
# its role is to wait for connection 
# from publisher and subscriber then 
# act as a middle man to make communication 
# between the two

SERV_PORT = 5001        # Default port for this broker
event = Event()         # For thread communication
DATA = ''               # Global variable to send data from pub to sub

# Thread function that handle connected subscriber
def handle_subscriber(s, ip, port):
    # Try retrieve incoming subscribe topic from this connection
    # Report disconnect and close connection if unable to communicate 
    try: 
        subscribe = s.recv(2048).decode('utf-8')
        print(port + ' Subscribe to: ' + subscribe)
    except socket.error:
        print('Subscriber '+ ip + ':' + port + ' disconnected...')
        s.close()
        return
    while True:
        event.wait()                # Notify this thread to wait for signal event from Publisher before continue 
        while event.is_set():       # Let this thread work until the event signal stop
            global DATA             # Use global variable--- receive from Publisher thread
            txtrecv = DATA          
            # In case of error from publisher side
            try:
                topic, data = txtrecv.split()
            except ValueError:
                print('Error: incorrect value received')
                continue
            # Only send message to this subscriber only if the topic is subscribed
            if topic == subscribe:
                try:
                    s.send(data.encode('utf-8'))                # Send data to subscriber
                except socket.error:
                    print('Error: Problem with socket connection at port: ' + ip)
                    s.close()
                    return
            time.sleep(0.6)                                     # Keep the thread from sending message more than once
    print('Subscriber '+ ip + ':' + port + ' disconnected...')
    s.close()                                                   # Close connection when done
    return 

# Thread function that handle Publisher
def handle_publisher(s, ip, port):
    while True:
        # Try receive message from publisher
        try:
            txtin = s.recv(2048).decode('utf-8')
            # If the received text is "quit" then terminate this thread and connection
            if txtin == b'quit':
                print('Publisher '+ ip + ':' + port + ' disconnected...')
                s.close
                return
            print(port + ' Published: ' + txtin)
            global DATA                     # Use global variable to communicate with Subscriber thread
            DATA = txtin
            # Signal the start of thread event
            event.set() 
            time.sleep(0.5)                 # Allow other thread to have a bit of time to work
            event.clear()
            # Signal the end of thread event
            DATA = ''

        except socket.error:
            break
    print('Publisher '+ ip + ':' + port + ' disconnected...')
    s.close()
    return

def main():
    addr = ('127.0.0.1', SERV_PORT)                 # Set default address for this Broker
    s = socket.socket(AF_INET, SOCK_STREAM)
    s.bind(addr)
    s.listen(5)                                     # Allow maximum of five communication
    print('MQTT borker started...')
    # Continuously listen on incoming connection 
    while True:
        sckt, addr = s.accept()                     # Accept incoming connection 
        ip, port = str(addr[0]),str(addr[1])
        clientType = sckt.recv(1024)                # First incoming message from client dignify its user type
        # If the client is subscriber
        if clientType == b'SUB':
            print('New subscriber connected from...' + ip + ':'  + port)
             # Try generate a thread for the target client
            try:
                sub = Thread(target=handle_subscriber, args=(sckt, ip, port))
                sub.daemon = True
                sub.start()
            except:
                print("Cannot start thread...")
                traceback.print_exc()
        # If the client is publisher
        elif clientType == b'PUB':
            print('New publisher connected from...' + ip + ':'  + port)
            # Try generate a thread for the target client
            try:
                pub = Thread(target=handle_publisher, args=(sckt, ip, port))
                pub.daemon = True
                pub.start()
            except:
                print("Cannot start thread...")
                traceback.print_exc()
        # In case of unknown type of user
        else:
            print('Unknown user type connected from...' + ip + ':'  + port)
    s.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print ('Interrupted ..')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
