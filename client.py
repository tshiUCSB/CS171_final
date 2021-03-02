# client that sends database operations to server

import json
import os
import socket
import sys
import threading

from blockchain import Blockchain, Operation, parse_op
from database import Database
from lamport import Lamport_clock

def logger(content, log=True):
	global PROCESS_ID

	if log:
		print("CLIENT" + PROCESS_ID + ": " + content)

def connect_clients(config):
	global PROCESS_ID
	sock_dict = {}

	for pid in config:
		port = config[pid]
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((socket.gethostname(), port))

		sock_dict[pid] = sock

	return sock_dict

def handle_exit():
	global gb_vars

	gb_vars["exit_flag"] = False
	sys.stdout.flush()
	os._exit(0)

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

		elif input_arr[0] == "send":
			tmp = input_arr[1].split(" ", 1)
			pid = tmp[0]
			if pid not in gb_vars["sock_dict"]:
				logger("cannot send to inexistent process " + pid)
			else:
				threading.Thread(target=send_msg, args=(pid, gb_vars["sock_dict"][pid], tmp[1])).start()

		else:
			print("invalid input")


if __name__ == "__main__":
	PROCESS_ID = sys.argv[1]
	PORT = sys.argv[2]

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

	threading.Thread(target=get_user_input, args=(config, )).start()



