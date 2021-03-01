# creation of node that is both server and client
# to participate in paxos in maintaining blockchain

import json
import os
import socket
import sys
import threading

from blockchain import blockchain
from database import Database
from lamport import Lamport_clock

def logger(content, log=True):
	global PROCESS_ID

	if log:
		print(PROCESS_ID + ": " + content)

def connect_clients(config):
	sock_dict = {}

	for pid in config:
		if pid != PROCESS_ID:
			port = config[pid]
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((socket.gethostname(), port))

			sock_dict[pid] = sock

	return sock_dict

def handle_exit():
	global gb_vars

	gb_vars["exit_flag"] = False
	sys.stdout.flush()
	gb_vars["in_sock"].close()
	os._exit(0)

def broadcast_msg(msg):
	global gb_vars

	for pid in gb_vars["sock_dict"]:
		sock = gb_vars["sock_dict"][pid]
		sock.sendall(bytes(msg))

	logger("broadcasted to all\n\tmsg: {}".format(msg))

def send_msg(pid, sock, msg):
	sock.sendall(bytes(msg))
	logger("sent to {}\n\tmsg: {}".format(pid, msg))

def get_user_input(config):
	global gb_vars
	sock_dict = gb_vars["sock_dict"]

	while not gb_vars["exit_flag"]:
		user_input = input()
		input_arr = user_input.split(" ", 1)

		if user_input == "connect" and len(sock_dict) < 1:
			sock_dict = connect_clients(config)

		elif user_input == "exit":
			handle_exit()

		elif input_arr[0] == "broadcast":
			threading.Thread(target=broadcast_msg, args=(input_arr[1])).start()

		elif input_arr[0] == "send":
			tmp = input_arr[1].split(" ", 1)
			pid = tmp[0]
			if pid not in config:
				logger("cannot send to inexistent process " + pid)
			else:
				threading.Thread(target=send_msg, args=(pid, sock_dict[pid], tmp[1]))

		else:
			print("invalid input")

def respond(stream, pid):
	global gb_vars

	while not gb_vars["exit_flag"]:
		try:
			data = stream.recv(1024)
		except:
			break
		if not data:
			stream.close()
			break

		logger("received from {}\n\tdata: {}".format(pid, data))


if __name__ == "__main__":
	PROCESS_ID = sys.argv[1]

	gb_vars = {
		"exit_flag": False,
		"lamport_clock": Lamport_clock(int(PROCESS_ID)),
		"sock_dict": {},
		"locks": {}
	}

	config_file_path = "config.json"
	config_file = open(config_file_path, "r")
	config = json.load(config_file)
	config_file.close()

	in_port = config[PROCESS_ID]
	in_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	in_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	in_sock.bind((socket.gethostname(), in_port))
	in_sock.listen(32)

	gb_vars["in_sock"] = in_sock

	threading.Thread(target=get_user_input, args=(config)).start()

	while not exit_flag:
		try:
			stream, addr = in_sock.accept()

			accecpted_pid = ""
			port = int(str(addr).split(":")[1])
			for pid in config:
				if config[pid] == port:
					accecpted_pid = pid
			logger("connection from {} at {}".format(accepted_pid, addr))

			threading.Thread(target=respond, args=(stream, accepted_pid)).start()
		except:
			break

