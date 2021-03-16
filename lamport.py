# definition of totally ordered lamport clock

class Lamport_Clock:
	def __init__(self, process_id, local_clock=0):
		self.local_clock = int(local_clock)
		self.process_id = int(process_id)

	def __eq__(self, rhs):
		if not isinstance(rhs, Lamport_Clock):
			# print(isinstance(rhs, Lamport_Clock))
			return False
		return self.local_clock == rhs.local_clock and self.process_id == rhs.process_id

	def __ne__(self, rhs):
		if not isinstance(rhs, Lamport_Clock):
			# print(isinstance(rhs, Lamport_Clock))
			return True
		# print("clock: {}".format(self.local_clock != rhs.local_clock))
		# print("pid: {}".format(self.process_id != rhs.process_id))
		return self.local_clock != rhs.local_clock or self.process_id != rhs.process_id

	def __lt__(self, rhs):
		if not isinstance(rhs, Lamport_Clock):
			return False
		if self.local_clock == rhs.local_clock:
			return self.process_id < rhs.process_id
		return self.local_clock < rhs.local_clock

	def __gt__(self, rhs):
		if not isinstance(rhs, Lamport_Clock):
			return False
		if self.local_clock == rhs.local_clock:
			return self.process_id > rhs.process_id
		return self.local_clock > rhs.local_clock

	def __str__(self):
		return "{}:{}".format(str(self.local_clock), str(self.process_id))

	def to_dict(self):
		return {
			"clock": self.local_clock,
			"pid": self.process_id
		}

	def update_clock(self, recv_clock):
		recv_local = 0
		if isinstance(recv_clock, dict):
			recv_local = recv_clock["clock"]
		elif isinstance(recv_clock, int):
			recv_local = recv_clock
		elif isinstance(recv_clock, Lamport_Clock):
			recv_local = recv_clock.local_clock
		recv_local = int(recv_local)

		self.local_clock = max(self.local_clock, recv_local) + 1

	def increment_clock(self):
		self.local_clock += 1

	def set_pid(self, pid):
		self.process_id = int(pid)

	def set_from_dict(self, d):
		self.local_clock = int(d["clock"])
		self.process_id = int(d["pid"])