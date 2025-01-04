import argparse
import signal
import socket

from tristar_adapter.services.offline import OfflineService
from tristar_adapter.services.online import OnlineService

server_sockets: list[socket.socket] = []
client_sockets: list[socket.socket] = []
workloads = ["ycsb", "tpcc", "smallbank"]

def prepare_for_connect():
    # Create a socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the address and port
    server_address = ('localhost', 7654)
    server_socket.bind(server_address)

    # Listen for connections
    server_socket.listen(1)
    print('Waiting for connection...')

    return server_socket


def signal_handler(signal, frame):
    close_service(server_sockets, client_sockets)


def close_service(server_ss: list[socket.socket], client_ss: list[socket.socket]):
    for s in server_ss:
        s.close()
    for s in client_ss:
        s.close()


# register signal handler
signal.signal(signal.SIGINT, signal_handler)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--workload", dest='wl', choices=workloads, type=str, required=True,
                        help="specify the workload")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    online_service = OnlineService(args.wl)
    offline_service = OfflineService(args.wl)
    server_socket = prepare_for_connect()
    server_sockets.append(server_socket)

    # Accept a connection
    client_socket, client_address = server_socket.accept()
    print('Connection established:', client_address)
    client_sockets.append(client_socket)

    # Receive and send messages
    while True:
        data = client_socket.recv(10240).decode()
        if not data:
            break
        print('Received message:', data)
        variables: list[str] = data.split(",")
        if variables[0].lower() == "online":
            res = online_service.service(variables[1], variables[2].strip())
            reply = str(res) + "\n"
            client_socket.sendall(reply.encode('utf-8'))
            print('Reply:', reply)
        elif variables[0].lower() == "offline":
            offline_service.service(variables[1], variables[2])
            reply = 'Train Finished!'
            client_socket.sendall(reply.encode('utf-8'))

    # Close the connection
    close_service(server_sockets, client_sockets)
