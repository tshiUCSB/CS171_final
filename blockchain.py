# definition of blockchain data structure

from hashlib import sha256

class Operation:
	def __init__(self, op, key, val=None):
		self.op = op
		self.key = key
		self.val = val

class Block:
	def __init__(self, op=None, prev=None):
		self.op = op
		self.prev = prev
		self.nonce = self.calc_nounce()

	def difficulty_check(self, block_hash):
		dig = block_hash[-1]
		for i in range(3):
			if dig == str(i):
				return True
		return False

	def calc_nounce(self):
		poss_chars = (" !\"#$%&'()*+-,-./"
			"0123456789"
			":;<=>?@"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			"[\\]^_`"
			"abcdefghijklmnopqrstuvwxyz"
			"{|}~")

		nounce_len = 1
		# TODO

class Blockchain:
	def __init__(self):
		self.head = Block()
		self.tail = head
		self.it = head

	def append(self, op):
		# TODO: stub
		return None

	