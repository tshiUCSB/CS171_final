# squash data structure for keeping track of operation ballots

import heapq
from threading import Lock

# class Queue:
# 	def __init__(self):
# 		# priority heap to keep track of order of ballots
# 		self.heap = []
# 		# hash table to associate ballot with acceptance counts
# 		self.hash_table = {}
# 		self.lock = Lock()

# 	def push(self, ballot):
# 		with self.lock:
# 			heapq.heappush(self.heap, ballot.num)
# 			key = str(ballot.num)
# 			self.hash_table[key] = ballot

# 	def pop(self):
# 		with self.lock:
# 			ballot_num = heapq.heappop(self.heap)
# 			key = str(ballot_num)
# 			ballot = self.hash_table.pop(key)
# 			return ballot

# 	def top(self):
# 		with self.lock:
# 			ballot_num = self.heap[0]
# 			key = str(ballot_num)
# 			return self.hash_table[key]

# 	def accept(self, ballot_num, pid):
# 		with self.lock:
# 			key = str(ballot_num)
# 			self.hash_table[key].acceptance.add(pid)
# 			return get_accept_count(ballot_num)

# 	def get_accept_count(self, ballot_num):
# 		with self.lock:
# 			key = str(ballot_num)
# 			return len(self.hash_table[key].acceptance)

class Queue:
	def __init__(self):
		# priority heap to keep track of order of ballots
		self.heap = []
		# hash table to associate ballot with acceptance counts
		self.hash_table = {}
		self.lock = Lock()

	def push(self, ballot):
		with self.lock:
			heapq.heappush(self.heap, ballot.num)
			key = str(ballot.num)
			self.hash_table[key] = ballot

	def pop(self):
		with self.lock:
			ballot_num = heapq.heappop(self.heap)
			key = str(ballot_num)
			ballot = self.hash_table.pop(key)
			return ballot

	def top(self):
		with self.lock:
			ballot_num = self.heap[0]
			key = str(ballot_num)
			return self.hash_table[key]

	def accept(self, ballot_num, pid):
		with self.lock:
			key = str(ballot_num)
			self.hash_table[key].acceptance.add(pid)
			return get_accept_count(ballot_num)

	def get_accept_count(self, ballot_num):
		with self.lock:
			key = str(ballot_num)
			return len(self.hash_table[key].acceptance)



			