# creation of node that servers operation requests from clients
# to participate in paxos in maintaining blockchain

import json
import os
import socket
import sys
import threading

from time import sleep

from blockchain import Blockchain, Ballot, Operation, Block
from database import KV_Store
from lamport import Lamport_Clock
from queue import Queue

def logger(content, log=True):
	global PROCESS_ID

	if log:
		print("{}: {}".format(PROCESS_ID, content))

def connect_clients(config):
	global gb_vars

	for pid in config:
		if pid != gb_vars["pid"]:
			port = config[pid]
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect((socket.gethostname(), port))
			logger("connecting to {}".format(pid))

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

def gather_promise(sock, data, pid):
	global gb_vars

	if gb_vars["phase"] != 1:
		return

	gb_vars["locks"]["ballot"].acquire()

	bal_num = data["bal_num"]
	curr_bal= gb_vars["ballot"]
	prom_bal_num = Lamport_Clock(bal_num["pid"], bal_num["clock"])

	if prom_bal_num != curr_bal.num:
		logger("promised ballot number {} doesn't match own ballot {}".format(prom_bal_num, curr_bal.num))
		return

	if "val" in data["accp"]:
		accp_bal = Ballot(0, 0)
		accp_bal.init_from_dict(data["accp"])
		if accp_bal > curr_bal:
			curr_bal.depth = accp_bal.depth
			curr_bal.max_num = accp_bal.num
			curr_bal.val = accp_bal.val

	curr_bal.acceptance.add(pid)
	logger("received {} / {} promises".format(len(curr_bal.acceptance), len(gb_vars["sock_dict"])))

	gb_vars["locks"]["ballot"].release()

	if len(curr_bal.acceptance) > len(gb_vars["sock_dict"]) // 2:
		logger("received promise from majority, transitioning to acceptance phase")
		
		curr_bal.acceptance.clear()
		gb_vars["phase"] = 2
		gb_vars["leader"] = gb_vars["pid"]

		if len(gb_vars["queue"]) > 0:
			request_acceptance()

def reply_client(req_num, res):
	global gb_vars

	sock = gb_vars["client_reqs"][req_num]
	opcode = "ACK" if res is None else "VAL"
	if res is None:
		res = {}

	data = {
		"result": res,
		"leader": gb_vars["leader"]
	}

	gb_vars["client_reqs"].pop(req_num)

	msg = {
		"opcode": opcode,
		"data": data
	}
	msg = json.dumps(msg)
	send_msg("client", sock, msg)

def decide_val(data):
	global gb_vars

	bal = gb_vars["ballot"]
	if isinstance(bal.val, Block):
		bal.val.decided = True

		res = gb_vars["db"].dispatch(bal.val.op)
		if res is None and bal.val.op.op == "get":
			res = "NO_KEY"

		op = gb_vars["queue"].pop(0)
		req_num = op[1]
		logger("request number: {} in {}".format(req_num, gb_vars["client_reqs"]))
		if req_num in gb_vars["client_reqs"]:
			threading.Thread(target=reply_client, args=(req_num, res)).start()
		else:
			data["req_num"] = req_num

	msg = {
		"opcode": "DEC",
		"data": data
	}
	msg = json.dumps(msg)
	broadcast_msg(msg)

	logger("decided on {}\n\tvalue: {}".format(bal.num, bal.val))

	bal.val = None
	bal.acceptance.clear()
	bal.max_num = bal.num
	if len(gb_vars["queue"]) > 0:
		request_acceptance()
	else:
		gb_vars["phase"] = 0

def gather_acceptance(sock, data, pid):
	global gb_vars

	if gb_vars["phase"] != 2:
		return

	gb_vars["locks"]["ballot"].acquire()

	bal_num = data["bal_num"]
	curr_bal= gb_vars["ballot"]
	accp_bal_num = Lamport_Clock(bal_num["pid"], bal_num["clock"])

	if accp_bal_num != curr_bal.num:
		logger("accepted ballot number {} doesn't match own ballot {}".format(prom_bal_num, curr_bal.num))
		return

	curr_bal.acceptance.add(pid)
	logger("received {} / {} acceptances".format(len(curr_bal.acceptance), len(gb_vars["sock_dict"])))

	gb_vars["locks"]["ballot"].release()

	if len(curr_bal.acceptance) > len(gb_vars["sock_dict"]) // 2:
		logger("received acceptance from majority, transitioning to decision phase")
		
		curr_bal.acceptance.clear()

		decide_val(data)

def handle_recv(sock, pid):
	if not gb_vars["sock_dict"][pid]["functional"]:
		logger("cannot receive from {} due to failed link".format(pid))
		return

	data = sock.recv(1024)
	if not data:
		return
	data = data.decode()
	logger("received from {}\n\tdata: {}".format(pid, data))
	data = json.loads(data)

	opcode = data["opcode"]
	opcode_dict = {
		"PROM": gather_promise,
		"ACCPD": gather_acceptance
	}

	threading.Thread(target=opcode_dict[opcode], args=(sock, data["data"], pid)).start()

def broadcast_msg(msg, recv=False):
	global gb_vars

	for pid in gb_vars["sock_dict"]:
		if pid == gb_vars["pid"]:
			continue
		if not gb_vars["sock_dict"][pid]["functional"]:
			logger("cannot send to {} due to failed link".format(pid))
			continue
		sock = gb_vars["sock_dict"][pid]["sock"]
		sock.sendall(bytes(msg, "utf-8"))

		if recv:
			threading.Thread(target=handle_recv, args=(sock, pid)).start()

	gb_vars["clock"].increment_clock()

	logger("broadcasted to all\n\tmsg: {}".format(msg))

def send_msg(pid, sock, msg):
	global gb_vars

	if pid != "client" and not gb_vars["sock_dict"][pid]["functional"]:
		logger("cannot send to {} due to failed link".format(pid))
		return

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

	pid = data
	gb_vars["sock_dict"][pid]["addr"] = addr
	gb_vars["addr_pid_map"][str(addr)] = pid

	# logger("{} : {}".format(addr, pid))

def prep_election():
	global gb_vars

	gb_vars["phase"] = 1

	bal = gb_vars["ballot"]
	bal.num.increment_clock()
	bal.num.set_pid(gb_vars["pid"])
	bal.depth = len(gb_vars["bc"]) + 1
	bal.val = None

	msg = {
		"opcode": "PREP",
		"data": {
			"depth": bal.depth,
			"bal_num": bal.num.to_dict()
		}
	}
	msg = json.dumps(msg)

	logger("prepping election phase for {}".format(bal.num))
	broadcast_msg(msg, True)

def request_acceptance():
	global gb_vars

	gb_vars["phase"] = 2

	bal = gb_vars["ballot"]
	bal.depth = len(gb_vars["bc"]) + 1
	data = {
		"bal_num": bal.num.to_dict(),
		"depth": bal.depth
	}
	if bal.val is None:
		op = Operation("", "")
		op.init_from_dict(gb_vars["queue"][0][0])
		print(str(op))
		# blk = gb_vars["bc"].append(op, "save_{}.pkl".format(gb_vars["pid"]))
		blk = gb_vars["bc"].append(op)
		print(str(blk))
		data["val"] = blk.to_dict()
		bal.val = blk
	else:
		data["val"] = bal.val

	msg = {
		"opcode": "ACCP",
		"data": data
	}
	msg = json.dumps(msg)

	logger("requesting acceptance")
	broadcast_msg(msg, True)

def forward_to_leader(data):
	global gb_vars

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
		data["req_num"] = key

	if not is_leader() and gb_vars["leader"] is not None:
		forward_to_leader(data)
	else:
		gb_vars["queue"].append((data, data["req_num"]))

		if gb_vars["leader"] is None:
			prep_election()
		elif is_leader() and len(gb_vars["queue"]) == 1:
			request_acceptance()
	# if gb_vars["leader"] is None:
	# 	if gb_vars["phase"] == 0:
	# 	prep_election(data)
	# elif is_leader():
	# 	gb_vars["queue"].append(data)
	# 	if len(gb_vars["queue"]) == 0:
	# 		request_acceptance()
	# else:
	# 	forward_to_leader(data)

def handle_prep_ballot(stream, addr, data):
	global gb_vars

	prep_bal = Ballot(Lamport_Clock(data["bal_num"]["pid"], data["bal_num"]["clock"]), None, data["depth"])
	curr_bal = gb_vars["ballot"]
	
	if prep_bal > curr_bal:
		gb_vars["ballot"].num = prep_bal.num

		pid = gb_vars["addr_pid_map"][str(addr)]
		gb_vars["leader"] = pid

		accp = gb_vars["accepted"]
		accp_dict = {}
		accp_dict["bal_num"] = accp["bal_num"].to_dict()
		if accp["val"] is not None:
			accp_dict["val"] = accp["val"]

		pid = gb_vars["addr_pid_map"][str(addr)]
		gb_vars["leader"] = pid

		msg = {
			"opcode": "PROM",
			"data": {
				"bal_num": data["bal_num"],
				"depth": data["depth"],
				"accp": accp_dict
			}
		}
		msg = json.dumps(msg)

		logger("promising {}".format(prep_bal.num))
		send_msg(pid, stream, msg)
	else:
		logger("rejected prep ballot {}".format(prep_bal.num))

def handle_accp_req(stream, addr, data):
	global gb_vars

	num = Lamport_Clock(0)
	num.init_from_dict(data["bal_num"])
	recv_bal = Ballot(num, None, data["depth"])
	bal = gb_vars["ballot"]
	accp = gb_vars["accepted"]

	if recv_bal > bal:
		if "op" in data["val"]:
			blk = Block("", d=data["val"])
			accp["val"] = gb_vars["bc"].append_block(blk)
		else:
			accp["val"] = recv_bal.val

		accp = gb_vars["accepted"]
		accp["bal_num"] = recv_bal.num

		msg = {
			"opcode": "ACCPD",
			"data": data
		}
		msg = json.dumps(msg)

		pid = gb_vars["addr_pid_map"][str(addr)]
		logger("accepted {}".format(recv_bal.num))
		send_msg(pid, stream, msg)
	else:
		logger("refused to accept {}".format(recv_bal.num))

def handle_decision(stream, addr, data):
	global gb_vars

	num = Lamport_Clock(0)
	num.init_from_dict(data["bal_num"])
	accp_num = gb_vars["accepted"]["bal_num"]
	if num != accp_num:
		logger("accepted ballot {} is not the same as decided ballot {}".format(accp_num, num))
		return

	accp = gb_vars["accepted"]
	if isinstance(accp["val"], Block):
		accp["val"].decided = True

		res = gb_vars["db"].dispatch(accp["val"].op)
		if res is None and accp["val"].op.op == "get":
			res = "NO_KEY"

		req_num = ""
		if "req_num" in data:
			req_num = data["req_num"]
		if req_num in gb_vars["client_reqs"]:
			threading.Thread(target=reply_client, args=(req_num, res)).start()

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

		if str(addr) in gb_vars["addr_pid_map"]:
			pid = gb_vars["addr_pid_map"][str(addr)]
			if not gb_vars["sock_dict"][pid]["functional"]:
				logger("cannot receive from {} due to failed link".format(pid))
				continue

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
			"PROP": handle_proposed_op,
			"PREP": handle_prep_ballot,
			"ACCP": handle_accp_req,
			"DEC": handle_decision
		}
		if opcode not in opcode_dict:
			logger("invalid opcode: " + opcode)
			continue

		threading.Thread(target=opcode_dict[opcode], args=(stream, addr, data["data"])).start()

if __name__ == "__main__":
	PROCESS_ID = sys.argv[1]

	gb_vars = {
		"accepted": {
			"bal_num": Lamport_Clock(0),
			"val": None
		},
		"addr_pid_map": {},
		"ballot": Ballot(Lamport_Clock(0), None),
		"bc": Blockchain(),
		"client_reqs": {},
		"clock": Lamport_Clock(int(PROCESS_ID)),
		"db": KV_Store(),
		"exit_flag": False,
		"leader": None,
		"locks": {
			"ballot": threading.Lock()
		},
		"pid": PROCESS_ID,
		"phase": 0,
		"queue": [],
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

