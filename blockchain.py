# definition of blockchain data structure

from hashlib import sha256
import json
import pickle

from database import Database

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
	def __init__(self, op, prev, tag=0):
		self.op = op
		self.prev = prev
		self.nonce = self.calc_nonce()
		# 0: tentative; 1: decided
		self.tag = tag

	def __str__(self):
		tag = "tentative" if self.tag == 0 else "decided"
		s = "\top: {}\n\thash: {}\n\tnonce: {}\n\ttag: {}".format(self.op, self.prev, self.nonce, tag)

	def concat(self):
		return str(self.op) + self.nonce + self.prev

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
		self.chain = []

	def __str__(self):
		s = "=====blockchain_start=====\n"
		for blk in self.chain:
			border = "----------\n"
			s += border + str(blk) + "\n"
		s += "=====blockchain_end====="

		return s

	def write_to_disk(self, save_file):
		save = open(save_file, 'w')
		pickle.dump(self.chain, save)
		save.close()

	def reconstruct_from_disk(self, save_file):
		save = open(save_file, 'r')
		self.chain = pickle.load(save)
		save.close()

		db = Database()
		for blk in self.chain:
			db.dispatch(blk.op)

		return db

	def append(self, op, save_file=None):
		prev_block = self.chain[-1]

		hasher = sha256()
		hasher.update(bytes(prev_block.concat(), "utf-8"))
		prev_hash = hasher.hexdigest()

		new_block = Block(op, prev_hash)
		self.chain.append(new_block)

		if save is not None:
			write_to_disk(save_file)




	