# creation of node that servers operation requests from clients
# to participate in paxos in maintaining blockchain

import json
import os
import socket
import sys
import threading

from blockchain import Blockchain
from database import KV_Store
from lamport import Lamport_Clock
from queue import Queue

def logger(content, log=True):
	global PROCESS_ID

	if log:
		print(PROCESS_ID + ": " + content)

def connect_clients(config):
	global gb_vars

	for pid in config:
		if pid != gb_vars["pid"]:
			port = config[pid]
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((socket.gethostname(), port))

			gb_vars["sock_dict"][pid]["sock"] = sock

			pid_msg = {
				"opcode": "PID",
				"data": gb_vars["pid"]
			}
			pid_msg = json.dumps(pid_msg)
			send_msg(pid, sock, pid_msg)

def handle_exit():
	global gb_vars

	gb_vars["exit_flag"] = False
	sys.stdout.flush()
	gb_vars["in_sock"].close()
	os._exit(0)

def broadcast_msg(msg):
	global gb_vars

	for pid in gb_vars["sock_dict"]:
		if pid == gb_vars["pid"]:
			continue
		sock = gb_vars["sock_dict"][pid]["sock"]
		sock.sendall(bytes(msg, "utf-8"))

	gb_vars["clock"].increment_clock()

	logger("broadcasted to all\n\tmsg: {}".format(msg))

def send_msg(pid, sock, msg):
	global gb_vars

	sock.sendall(bytes(msg, "utf-8"))
	gb_vars["clock"].increment_clock()

	logger("sent to {}\n\tmsg: {}".format(pid, msg))

def parse_command(cmd):
	global gb_vars

	cmd_lst = cmd.split("(")
	func = cmd_lst[0]

	if func == "failProcess":
		logger("process failed")
		handle_exit()

	elif func == "printBlockchain":
		logger(gb_vars["bc"])
	elif func == "printKVStore":
		logger(gb_vars["db"])
	elif func == "printQueue":
		logger(gb_vars["queue"])

	elif func == "failLink" or func == "fixLink":
		if len(cmd_lst) < 2:
			print("no arguments provided for " + func)
			return

		args = cmd_lst[1][ :-1].split(", ")
		if len(args) != 2:
			print("expected 2 arguments for " + func + ", received " + str(len(args)))
			return

		if args[0] != PROCESS_ID:
			print("source argument {} is not the current process {}".format(args[0], PROCESS_ID))
			return

		if args[1] == PROCESS_ID:
			print("destination argument cannot be current process " + PROCESS_ID)
			return

		functional = False if func == "failLink" else True
		gb_vars["sock_dict"][args[1]]["functional"] = functional

		if functional:
			logger("restablished connection with " + args[1])
		else:
			logger("connection to {} failed".format(args[1]))

	else:
		print("invalid input")

def get_user_input(config):
	global gb_vars

	while not gb_vars["exit_flag"]:
		user_input = input()
		input_arr = user_input.split(" ", 1)

		if user_input == "connect":
			connect_clients(config)

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
			parse_command(user_input)

def is_leader():
	global gb_vars
	return gb_vars["pid"] == gb_vars["leader"]

def match_pid(stream, addr, data):
	global gb_vars

	pid = data["data"]
	gb_vars["sock_dict"][pid]["addr"] = addr
	gb_vars["addr_pid_map"][str(addr)] = pid

	# logger("{} : {}".format(addr, pid))

def prep_election(data):


def acknowledge_ballot(data):


def forward_to_leader(data):
	global gb_vars

	data["req_num"] = key
	msg = {
		"opcode": "PROP",
		"data": data
	}
	msg = json.dumps(msg)

	lead_sock = gb_vars["sock_dict"][gb_vars["leader"]]["sock"]
	send_msg(gb_vars["leader"], lead_sock, msg)

def handle_proposed_op(stream, addr, data):
	global gb_vars

	key = str(gb_vars["clock"])
	if "req_num" not in data:
		gb_vars["client_reqs"][key] = stream

	if gb_vars["leader"] is None:
		prep_election()
	elif is_leader():
		acknowledge_ballot()
	else:
		forward_to_leader(key, data)


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

		data = data.decode()
		logger("received from {}\n\tdata: {}".format(addr, data))

		data = json.loads(data)

		if "clock" in data:
			gb_vars["clock"].update_clock(data["clock"])
		else:
			gb_vars["clock"].increment_clock()
		
		opcode = data["opcode"]
		opcode_dict = {
			"PID": match_pid,
			"PROP": handle_proposed_op
		}
		if opcode not in opcode_dict:
			logger("invalid opcode: " + opcode)
			continue

		threading.Thread(target=opcode_dict[opcode], args=(stream, addr, data["data"])).start()

if __name__ == "__main__":
	PROCESS_ID = sys.argv[1]

	gb_vars = {
		"accepted": {
			"ballot": Lamport_Clock(0),
			"val": None
		},
		"addr_pid_map": {},
		"ballot_num": Lamport_Clock(0),
		"bc": Blockchain(),
		"client_reqs": {},
		"clock": Lamport_Clock(int(PROCESS_ID)),
		"db": KV_Store(),
		"exit_flag": False,
		"leader": None,
		"locks": {},
		"pid": PROCESS_ID,
		"queue": Queue(),
		"sock_dict": {}
	}

	config_file_path = "config.json"
	config_file = open(config_file_path, "r")
	config = json.load(config_file)
	config_file.close()

	for pid in config:
		if pid != PROCESS_ID:
			port = config[pid]
			sock_info = {
				"sock": None,
				"functional": True,
				"addr": 0
			}
			gb_vars["sock_dict"][pid] = sock_info

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

