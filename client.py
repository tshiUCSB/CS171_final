# client that sends database operations to server

import json
import os
import socket
import sys
import threading

from random import randint

from blockchain import Blockchain, Operation, parse_op
from database import KV_Store
from lamport import Lamport_Clock

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

def await_msg(sock):
	data = sock.recv(1024)
	if not data:
		return

def send_msg(pid, sock, msg):
	sock.sendall(bytes(msg, "utf-8"))
	logger("sent to {}\n\tmsg: {}".format(pid, msg))

	await_msg(sock);

def req_operation(config, op, key, val=None):
	global gb_vars

	lead = gb_vars["est_lead"]
	lead_sock = None

	if lead is None:
		# lead = str(randint(1, len(config)))
		lead = "1"
		gb_vars["est_lead"] = lead
	
		lead_port = config[lead]
		lead_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		lead_sock.connect((socket.gethostname(), lead_port))
		gb_vars["sock"] = lead_sock
	else:
		lead_sock = gb_vars["sock"]

	content = {
		"op": op,
		"key": key
	}
	if val is not None:
		content["val"] = val

	msg = {
		"opcode": "PROP",
		"data": content
	}
	msg = json.dumps(msg)

	send_msg(lead, lead_sock, msg)


def get_user_input(config):
	global gb_vars

	while not gb_vars["exit_flag"]:
		user_input = input()
		input_arr = user_input.split(" ", 1)
		cmd_arr = user_input.split("(", 1)

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

		elif cmd_arr[0] == "Operation":
			args = cmd_arr[1][ :-1].split(", ")

			if args[0] != "put" and args[0] != "get":
				print("invalid operation " + args[0])
				continue
			if args[0] == "get" and len(args) != 2:
				print("expected 2 arguments for get operation, received " + str(len(args)))
				continue

			if args[0] == "put" and len(args) != 3:
				print("expected 3 arguments for put operation, received " + str(len(args)))
				continue

			args.insert(0, config)

			threading.Thread(target=req_operation, args=tuple(args)).start()

		else:
			print("invalid input")


if __name__ == "__main__":
	PROCESS_ID = sys.argv[1]
	PORT = sys.argv[2]

	gb_vars = {
		"est_lead": None,
		"exit_flag": False,
		"lamport_clock": Lamport_Clock(int(PROCESS_ID)),
		"sock": None,
		"sock_dict": {},
		"timeout": False
	}

	config_file_path = "config.json"
	config_file = open(config_file_path, "r")
	config = json.load(config_file)
	config_file.close()

	threading.Thread(target=get_user_input, args=(config, )).start()



