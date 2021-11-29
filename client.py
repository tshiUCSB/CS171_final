# client that sends database operations to server

import json
import os
import socket
import sys
import threading

from random import randint
from time import sleep

from blockchain import Blockchain, Operation, parse_op
from database import KV_Store
from lamport import Lamport_Clock

def logger(content, log=True):
	global PROCESS_ID

	if log:
		print("CLIENT{}: {}".format(PROCESS_ID, content))

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
	if gb_vars["sock"] is not None:
		gb_vars["sock"].close()
	sys.stdout.flush()
	os._exit(0)

def get_rand_lead():
	global config
	return list(config.keys())[randint(1, len(config)) - 1]

def switch_leader(lead="1"):
	global gb_vars

	if gb_vars["est_lead"] != lead:

		logger("switching connection from {} to {}".format(gb_vars["est_lead"], lead))

		gb_vars["est_lead"] = lead

		port = config[gb_vars["est_lead"]]
		gb_vars["sock"] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		gb_vars["sock"].connect((socket.gethostname(), port))

def timeout(retry, opt):
	global gb_vars
	global config

	sleep(gb_vars["timeout"][0])
	if gb_vars["timeout"][1] and gb_vars["queue"][0] == opt:
		logger("operation timed out after {} seconds".format(gb_vars["timeout"][0]))
		if not retry:
			req_operation(retry=True, opt=opt)
		else:
			lead = get_rand_lead()
			req_operation(retry=True, switch_lead=lead, opt=opt)

def await_msg(sock, retry=False, opt=None):
	global gb_vars
	global config

	if gb_vars["timeout"][0] is not None:
		gb_vars["timeout"][1] = True
		threading.Thread(target=timeout, args=(retry, opt)).start()

		try:
			data = sock.recv(1024)
		except:
			gb_vars["timeout"][1] = False

			if gb_vars["est_lead"] in config:
				config.pop(gb_vars["est_lead"])
			lead = get_rand_lead()
			req_operation(retry=False, switch_lead=lead)
			return

	gb_vars["timeout"][1] = False

	if not data:
		logger("connection to {} closed".format(gb_vars["est_lead"]))

		if len(gb_vars["queue"]) > 0:
			if gb_vars["est_lead"] in config:
				config.pop(gb_vars["est_lead"])
			lead = get_rand_lead()
			req_operation(retry=False, switch_lead=lead)
		return
	if (len(gb_vars["queue"])) != 0 and (opt is None or opt == gb_vars["queue"][0]):
		gb_vars["queue"].pop(0)
	data = data.decode()

	logger("received {}".format(data))

	data = json.loads(data)
	if "leader" in data["data"]:
		switch_leader(lead=data["data"]["leader"])

	if len(gb_vars["queue"]) > 0:
		req_operation()

def send_msg(pid, sock, msg, retry=False, opt=None):
	global gb_var

	try:
		sock.sendall(bytes(msg, "utf-8"))
	except:
		logger("exception from send to " + pid)

		if gb_vars["est_lead"] in config:
			config.pop(gb_vars["est_lead"])
		lead = get_rand_lead()
		switch_leader(lead)
		send_msg(lead, gb_vars["sock"], msg, retry, opt)
		return

	logger("sent to {}\n\tmsg: {}".format(pid, msg))

	await_msg(sock, retry=retry, opt=opt);

def req_operation(retry=False, switch_lead=None, opt=None):
	global gb_vars
	global config

	if len(gb_vars["queue"]) < 1:
		return

	lead = gb_vars["est_lead"]
	lead_sock = None

	if lead is None:
		lead = get_rand_lead()
		# lead = "1"
		gb_vars["est_lead"] = lead

		logger("connecting to " + lead)

	
		lead_port = config[lead]
		lead_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		lead_sock.connect((socket.gethostname(), lead_port))
		gb_vars["sock"] = lead_sock
	else:
		lead_sock = gb_vars["sock"]

	if switch_lead is not None:
		switch_leader(lead=switch_lead)

	top = gb_vars["queue"][0]
	op, key, val = top[0]

	if isinstance(val, str):
		val = json.loads(val)

	content = {
		"op": op,
		"key": key,
		"val": val
	}

	opcode = "PROP"
	if retry:
		opcode = "LEAD"
	msg = {
		"opcode": opcode,
		"data": content
	}
	msg = json.dumps(msg)

	send_msg(lead, lead_sock, msg, retry=retry, opt=top)


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

			if len(args) == 2:
				args.append({})

			gb_vars["queue"].append((args, str(gb_vars["clock"])))
			gb_vars["clock"].increment_clock()

			if len(gb_vars["queue"]) == 1:
				threading.Thread(target=req_operation).start()

		else:
			print("invalid input")


if __name__ == "__main__":
	PROCESS_ID = sys.argv[1]
	PORT = sys.argv[2]
	
	t_out = [None, False]
	if len(sys.argv) > 3:
		t_out[0] = int(sys.argv[3])

	gb_vars = {
		"est_lead": None,
		"exit_flag": False,
		"clock": Lamport_Clock(int(PROCESS_ID)),
		"locks": {},
		"queue": [],
		"sock": None,
		"sock_dict": {},
		"timeout": t_out
	}

	gb_vars["locks"]["sock"] = threading.Lock()

	config_file_path = "config.json"
	config_file = open(config_file_path, "r")
	config = json.load(config_file)
	config_file.close()

	get_user_input(config)



