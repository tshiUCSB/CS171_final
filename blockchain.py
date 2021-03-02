# definition of blockchain data structure

from hashlib import sha256
import json

class Operation:
	def __init__(self, op, key, val={}):
		self.op = op
		self.key = key
		self.val = val

	def __str__(self):
		s = op + "-" + key + "-" + str(val)
		return s

def parse_op(op_str):
	op_arr = str.split("-", 2)
	val = json.loads(op_arr[2])
	return Operation(op_arr[0], op_arr[1], val)

class Block:
	def __init__(self, op, prev):
		self.op = op
		self.prev = prev
		self.nonce = self.calc_nonce()
		# 0: tentative; 1: decided
		self.tag = 0

	def difficulty_check(self, nonce):
		hasher = sha256()
		hasher.update(bytes(str(self.op) + nonce), "utf-8")
		block_hash = hasher.hexdigest()
		dig = block_hash[-1]
		for i in range(3):
			if dig == str(i):
				return True
		return False

	def calc_nonce_helper(self, nonce, poss_chars, max_len):
		if len(nonce) == max_len:
			return None

		for c in poss_chars:
			n = nonce + c
			if self.difficulty_check(n):
				return n
		
		for c in poss_chars:
			n = self.calc_nonce_helper(nonce + c, poss_chars, max_len)
			if n is not None:
				return n

		return None

	def calc_nonce(self, max_len=10):
		poss_chars = (" !\"#$%&'()*+-,-./"
			"0123456789"
			":;<=>?@"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			"[\\]^_`"
			"abcdefghijklmnopqrstuvwxyz"
			"{|}~")

		return self.calc_nonce_helper("", poss_chars, max_len)

class Blockchain:
	def __init__(self):
		self.head = None
		self.tail = head
		self.it = head

	def append(self, op):
		# TODO: stub
		return None

	