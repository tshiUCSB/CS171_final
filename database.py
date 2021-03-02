# definition of key-store database

from threading import Lock

class Database:
	def __init__(self):
		self.store = {}
		self.lock = Lock()

	def __str__(self):
		return str(self.store)

	def dispatch(self, op):
		result = None

		if op.op == "put":
			self.put(op.key, op.val)
		elif op.op == "get":
			result = self.get(op.key)
		else:
			raise Exception("invalid operation {}".format(op.op))

		return result

	def put(self, key, val):
		# val format {"phone_number": "111-222-3333"}
		with self.lock:
			self.store[key] = val

	def get(self, key):
		with self.lock:
			return self.store[key]