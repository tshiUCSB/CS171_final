# definition of totally ordered lamport clock

class Lamport_Clock:
	def __init__(self, process_id, local_clock=0):
		self.local_clock = int(local_clock)
		self.process_id = int(process_id)

	def __eq__(self, rhs):
		if not isinstance(rhs, Lamport_clock):
			return False
		return self.local_clock == rhs.local_clock and self.process_id == rhs.process_id

	def __lt__(self, rhs):
		if not isinstance(rhs, Lamport_clock):
			return False
		if self.local_clock == rhs.local_clock:
			return self.process_id < rhs.process_id
		return self.local_clock < rhs.local_clock

	def __gt__(self, rhs):
		if not isinstance(rhs, Lamport_clock):
			return False
		if self.local_clock == rhs.local_clock:
			return self.process_id > rhs.process_id
		return self.local_clock > rhs.local_clock

	def __str__(self):
		return "{}:{}".format(str(self.local_clock), str(self.process_id))

	def update_clock(self, recv_clock):
		self.local_clock = max(self.local_clock, recv_clock.local_clock) + 1

	def increment_clock(self):
		self.local_clock += 1