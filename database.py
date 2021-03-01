# definition of key-store database

class Database:
	def __init__(self):
		self.store = {}

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
		self.store[key] = val

	def get(self, key):
		return self.store[key]