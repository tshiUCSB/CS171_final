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

	def calc_nounce(self):


class Blockchain:
	def __init__(self):
		self.head = Block()
		self.tail = head
		self.it = head

	def append(self, op):
		# TODO: stub
		return None

	