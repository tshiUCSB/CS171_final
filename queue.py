# squash data structure for keeping track of operation ballots

import heapq

class Queue:
	def __init__(self):
		self.heap = []
		self.hash_table = {}

	def push(self, ballot):
		heapq.heappush(self.heap, ballot.num)
		key = str(ballot.num)
		self.hash_table[key] = ballot

	def pop(self):
		ballot_num = heapq.heappop(self.heap)
		key = str(ballot_num)
		ballot = self.hash_table.pop(key)
		return ballot

	def top(self):
		ballot_num = self.heap[0]
		key = str(ballot_num)
		return self.hash_table[key]

	def accept(self, ballot_num, pid):
		key = str(ballot_num)
		self.hash_table[key].acceptance.add(pid)
		return get_accept_count(ballot_num)

	def get_accept_count(self, ballot_num):
		key = str(ballot_num)
		return len(self.hash_table[key].acceptance)

