# creation of node that servers operation requests from clients
# to participate in paxos in maintaining blockchain

import json
import os
import socket
import sys
import threading

from blockchain import Blockchain
from database import Database
from lamport import Lamport_clock
from queue import Queue

def logger(content, log=True):
	global PROCESS_ID

	if log:
		print(PROCESS_ID + ": " + content)

def connect_clients(config):
	global PROCESS_ID
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
	global PROCESS_ID

	for pid in gb_vars["sock_dict"]:
		if pid == PROCESS_ID:
			continue
		sock = gb_vars["sock_dict"][pid]
		sock.sendall(bytes(msg, "utf-8"))

	logger("broadcasted to all\n\tmsg: {}".format(msg))

def send_msg(pid, sock, msg):
	sock.sendall(bytes(msg, "utf-8"))
	logger("sent to {}\n\tmsg: {}".format(pid, msg))

def get_user_input(config):
	global gb_vars

	while not gb_vars["exit_flag"]:
		user_input = input()
		input_arr = user_input.split(" ", 1)

		if user_input == "connect" and len(gb_vars["sock_dict"]) < 1:
			gb_vars["sock_dict"] = connect_clients(config)

		elif user_input == "exit":
			handle_exit()

		elif input_arr[0] == "broadcast":
			threading.Thread(target=broadcast_msg, args=(input_arr[1], )).start()

		elif input_arr[0] == "send":
			tmp = input_arr[1].split(" ", 1)
			pid = str(tmp[0])
			if pid not in gb_vars["sock_dict"]:
				logger("cannot send to inexistent process " + pid)
			else:
				threading.Thread(target=send_msg, args=(pid, gb_vars["sock_dict"][pid], tmp[1])).start()

		else:
			print("invalid input")

def respond(stream, addr):
	global gb_vars

	while not gb_vars["exit_flag"]:
		try:
			data = stream.recv(1024)
		except:
			break
		if not data:
			stream.close()
			logger("closed stream from {}".format(addr))
			break

		logger("received from {}\n\tdata: {}".format(addr, data))


if __name__ == "__main__":
	PROCESS_ID = sys.argv[1]

	gb_vars = {
		"accepted": {
			"ballot": Lamport_clock(0),
			"val": None
		},
		"exit_flag": False,
		"lamport_clock": Lamport_clock(int(PROCESS_ID)),
		"locks": {},
		"sock_dict": {}
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

	threading.Thread(target=get_user_input, args=(config, )).start()

	while not gb_vars["exit_flag"]:
		# try:
		stream, addr = in_sock.accept()
		logger("connection from {}".format(addr))

		threading.Thread(target=respond, args=(stream, addr)).start()
		# except:
		# 	break

