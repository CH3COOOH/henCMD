import json
import os
import shlex
import time
import subprocess
import threading
import queue
from websocket_server import WebsocketServer
import azstd.out
import azstd.file.json

class WServer:
	def __init__(self, conf_path):
		self.conf = azstd.file.json.gracefulLoadJSON(conf_path)
		if not isinstance(self.conf, dict):
			raise ValueError("configuration must contain a JSON object")

		missing = [key for key in ('host', 'port', 'log_level') if key not in self.conf]
		if missing:
			raise ValueError(f"missing required configuration field(s): {', '.join(missing)}")

		self.host = self.conf['host']
		self.port = self.conf['port']
		self.log_level = self.conf['log_level']

		if not isinstance(self.host, str) or not self.host.strip():
			raise ValueError("host must be a non-empty string")
		if not isinstance(self.port, int) or isinstance(self.port, bool) or not 1 <= self.port <= 65535:
			raise ValueError("port must be an integer between 1 and 65535")
		if not isinstance(self.log_level, int) or isinstance(self.log_level, bool):
			raise ValueError("log_level must be an integer")

		self.timeout_seconds = self.conf.get('timeout_seconds', 300)
		if not isinstance(self.timeout_seconds, (int, float)) or isinstance(self.timeout_seconds, bool) or self.timeout_seconds <= 0:
			raise ValueError("timeout_seconds must be a positive number")

		exec_conf = self.conf.get('exec')
		if exec_conf is not None:
			if not isinstance(exec_conf, dict):
				raise ValueError("exec must be a JSON object or null")
			normalized_exec = {}
			for name, command in exec_conf.items():
				if not isinstance(name, str) or not name.strip():
					raise ValueError("exec command names must be non-empty strings")
				if isinstance(command, str):
					if not command.strip():
						raise ValueError(f"exec command '{name}' must be a non-empty string")
					args = shlex.split(command, posix=False)
					args = [self._strip_surrounding_quotes(arg) for arg in args]
				elif isinstance(command, list):
					if not command:
						raise ValueError(f"exec command '{name}' must not be empty")
					if not all(isinstance(arg, str) for arg in command):
						raise ValueError(f"exec command '{name}' arguments must be strings")
					args = command.copy()
				else:
					raise ValueError(f"exec command '{name}' must be a string or an array")
				if not args or not args[0].strip():
					raise ValueError(f"exec command '{name}' must contain a valid executable")
				normalized_exec[name] = args
			self.conf['exec'] = normalized_exec

		self.log_encoding = 'ansi'
		if 'log_encoding' in self.conf:
			self.log_encoding = self.conf['log_encoding']
			if not isinstance(self.log_encoding, str) or not self.log_encoding.strip():
				raise ValueError("log_encoding must be a non-empty string")
		self.log = azstd.out.Log(show_level=self.log_level)
		self.active_processes = {}
		self.active_processes_lock = threading.Lock()

	def _strip_surrounding_quotes(self, value):
		if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
			return value[1:-1]
		return value

	def _parse_user_args(self, data):
		if not isinstance(data, str):
			raise ValueError("command data must be a string")
		if not data.strip():
			return []
		args = shlex.split(data, posix=(os.name != 'nt'))
		return [self._strip_surrounding_quotes(arg) for arg in args]

	def _close_session(self, client):
		client["handler"].send_close(1000, b'')

	def _send_msg(self, server, client, msg):
		server.send_message(client, msg)

	def _drain_output(self, server, client, output_queue):
		while True:
			try:
				line = output_queue.get_nowait()
			except queue.Empty:
				return
			if line is None:
				return
			self._send_msg(server, client, line)

	def _terminate_process(self, process):
		if process.poll() is not None:
			return
		process.terminate()
		try:
			process.wait(timeout=5)
		except subprocess.TimeoutExpired:
			process.kill()
			process.wait(timeout=5)

	def _client_left(self, client, server):
		client_id = client["id"]
		with self.active_processes_lock:
			record = self.active_processes.pop(client_id, None)
		if record is not None:
			record["cancelled"].set()
			self._terminate_process(record["process"])
			self.log.print(f"Client disconnected. Cancelled task for client {client_id}.", 1)

	def _close_and_report(self, server, client, show_console, show_client, loglv=3):
		self.log.print(show_console, loglv)
		try:
			self._send_msg(server, client, show_client)
		except Exception as e:
			self.log.print(f"Unable to send response to client: {e}", 3)
		try:
			self._close_session(client)
		except Exception as e:
			self.log.print(f"Unable to close client session: {e}", 3)

	def _msgReceived(self, client, server, msg):
		try:
			d_msg = json.loads(msg)
			data_type = d_msg['type']
		except Exception as e:
			self._close_and_report(server, client, f"Unable to parse msg: {msg}. {e}", 'Data format error.')
			return -1
		if data_type == 'cmd':
			try:
				exec_conf = self.conf.get('exec')
				if exec_conf is None:
					self._close_and_report(server, client, 'No command defined.', 'Server config error.')
					return -1
				if not isinstance(exec_conf, dict):
					self._close_and_report(server, client, 'Invalid command configuration.', 'Server config error.')
					return -1
				param = d_msg.get('param')
				if param not in exec_conf:
					self._close_and_report(server, client, f"Unknown command: {param}", 'Invalid command.')
					return -1
				try:
					user_args = self._parse_user_args(d_msg.get('data'))
				except ValueError as e:
					self._close_and_report(server, client, f"Invalid command arguments: {e}", 'Invalid command arguments.')
					return -1
				exec_args = exec_conf[param].copy()
				exec_args.extend(user_args)
				self.log.print(f"EXEC > {' '.join(exec_args)}", 2)
				p = subprocess.Popen(exec_args,
					shell=False,
					stdout=subprocess.PIPE,
					stderr=subprocess.STDOUT,
					text=True,
					encoding=self.log_encoding,
					errors="ignore",
					bufsize=4096)
				cancelled = threading.Event()
				client_id = client["id"]
				record = {"process": p, "cancelled": cancelled}
				with self.active_processes_lock:
					self.active_processes[client_id] = record

				output_queue = queue.Queue()
				def read_output():
					try:
						for line in p.stdout:
							output_queue.put(line.strip())
					finally:
						output_queue.put(None)
				reader = threading.Thread(target=read_output, daemon=True)
				reader.start()

				deadline = time.monotonic() + self.timeout_seconds
				reason = None
				while p.poll() is None:
					self._drain_output(server, client, output_queue)
					if cancelled.is_set():
						reason = "cancelled"
						self._terminate_process(p)
						break
					if time.monotonic() >= deadline:
						reason = "timeout"
						self.log.print(f"Task timed out after {self.timeout_seconds} seconds.", 2)
						self._terminate_process(p)
						break
					time.sleep(.05)

				self._drain_output(server, client, output_queue)
				reader.join(timeout=1)
				p.wait()

				with self.active_processes_lock:
					if self.active_processes.get(client_id) is record:
						self.active_processes.pop(client_id, None)

				if reason == "cancelled":
					return -1
				if reason == "timeout":
					self._close_and_report(server, client, f"Task timed out after {self.timeout_seconds} seconds.", f"Task timed out after {self.timeout_seconds} seconds.", loglv=2)
					return -1
				self._close_and_report(server, client, f"Task completed.", 'Done.', loglv=1)
				return 0
			except Exception as e:
				self._close_and_report(server, client, f"**Error occured. Detail: \n-----\n{e}", 'Server error.')
				return -1
		elif data_type == 'hello':
			exec_conf = self.conf.get('exec')
			command_names = [str(k) for k in exec_conf.keys()] if isinstance(exec_conf, dict) else []
			self._close_and_report(server, client, f"New commander online. CMD menu provided.", json.dumps(command_names), 1)
		else:
			self._close_and_report(server, client, f"Unknown message type: {data_type}", 'Unknown message type.')
		
	def start(self):
		server = WebsocketServer(port=self.port, host=self.host)
		server.set_fn_message_received(self._msgReceived)
		server.set_fn_client_left(self._client_left)
		self.log.print(f"Listening on {self.host}:{self.port}...", 1)
		server.run_forever()
