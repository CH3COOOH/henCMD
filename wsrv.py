import json
import os
import shlex
import stat
import tempfile
import time
import ipaddress
import subprocess
import threading
import queue
from websocket_server import WebsocketServer
import azstd.out

class WServer:
	def __init__(self, conf_path):
		self.config_path = os.path.abspath(conf_path)
		with open(self.config_path, 'rb') as config_file:
			initial_content = config_file.read()
		self._config_lock = threading.Lock()
		self._last_good_content = initial_content
		config, ip_whitelist, whitelist_enabled = self._parse_config(initial_content)
		self._apply_config(config, ip_whitelist, whitelist_enabled)
		self.log = azstd.out.Log(show_level=config['log_level'])
		self.active_processes = {}
		self.active_processes_lock = threading.Lock()
		self._config_watch_stop = threading.Event()

	def _parse_config(self, content):
		conf = json.loads(content.decode('utf-8-sig'))
		if not isinstance(conf, dict):
			raise ValueError("configuration must contain a JSON object")

		missing = [key for key in ('host', 'port', 'log_level') if key not in conf]
		if missing:
			raise ValueError(f"missing required configuration field(s): {', '.join(missing)}")

		conf = conf.copy()
		host = conf['host']
		port = conf['port']
		log_level = conf['log_level']

		if not isinstance(host, str) or not host.strip():
			raise ValueError("host must be a non-empty string")
		if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
			raise ValueError("port must be an integer between 1 and 65535")
		if not isinstance(log_level, int) or isinstance(log_level, bool):
			raise ValueError("log_level must be an integer")

		timeout_seconds = conf.get('timeout_seconds', 300)
		if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool) or timeout_seconds <= 0:
			raise ValueError("timeout_seconds must be a positive number")

		whitelist_enabled = 'ip_whitelist' in conf
		ip_whitelist = []
		if whitelist_enabled:
			whitelist_entries = conf['ip_whitelist']
			if not isinstance(whitelist_entries, list):
				raise ValueError("ip_whitelist must be an array")
			for entry in whitelist_entries:
				if not isinstance(entry, str) or not entry.strip():
					raise ValueError("ip_whitelist entries must be non-empty strings")
				try:
					ip_whitelist.append(ipaddress.ip_network(entry.strip(), strict=False))
				except ValueError as e:
					raise ValueError(f"invalid ip_whitelist entry '{entry}': {e}")

		exec_conf = conf.get('exec')
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
			conf['exec'] = normalized_exec

		log_encoding = conf.get('log_encoding', 'ansi')
		if not isinstance(log_encoding, str) or not log_encoding.strip():
			raise ValueError("log_encoding must be a non-empty string")
		return conf, ip_whitelist, whitelist_enabled

	def _apply_config(self, conf, ip_whitelist, whitelist_enabled):
		runtime_config = {
			'conf': conf,
			'host': conf['host'],
			'port': conf['port'],
			'log_level': conf['log_level'],
			'timeout_seconds': conf.get('timeout_seconds', 300),
			'log_encoding': conf.get('log_encoding', 'ansi'),
			'ip_whitelist': ip_whitelist,
			'ip_whitelist_enabled': whitelist_enabled,
		}
		with self._config_lock:
			self._runtime_config = runtime_config
			self.conf = conf
			self.host = runtime_config['host']
			self.port = runtime_config['port']
			self.log_level = runtime_config['log_level']
			self.timeout_seconds = runtime_config['timeout_seconds']
			self.log_encoding = runtime_config['log_encoding']
			self.ip_whitelist = ip_whitelist
			self.ip_whitelist_enabled = whitelist_enabled
			if hasattr(self, 'log'):
				self.log.show_level = self.log_level

	def _config_snapshot(self):
		with self._config_lock:
			return self._runtime_config

	def _restore_last_good_config(self):
		config_dir = os.path.dirname(self.config_path)
		try:
			permissions = stat.S_IMODE(os.stat(self.config_path).st_mode)
		except FileNotFoundError:
			permissions = None

		fd, temp_path = tempfile.mkstemp(prefix='.henCMD-config-', suffix='.tmp', dir=config_dir)
		try:
			with os.fdopen(fd, 'wb') as temp_file:
				temp_file.write(self._last_good_content)
				temp_file.flush()
				os.fsync(temp_file.fileno())
			if permissions is not None:
				os.chmod(temp_path, permissions)
			os.replace(temp_path, self.config_path)
		finally:
			if os.path.exists(temp_path):
				os.remove(temp_path)

	def _reload_config_once(self):
		try:
			with open(self.config_path, 'rb') as config_file:
				content = config_file.read()
		except FileNotFoundError:
			time.sleep(.25)
			if os.path.exists(self.config_path):
				return
			self.log.print("Configuration file was removed; restoring the last known-good config.", 2)
			try:
				self._restore_last_good_config()
			except OSError as e:
				self.log.print(f"Unable to restore configuration file: {e}", 3)
			return
		except OSError as e:
			self.log.print(f"Unable to read configuration file: {e}", 3)
			return

		if content == self._last_good_content:
			return

		try:
			config, ip_whitelist, whitelist_enabled = self._parse_config(content)
			current = self._config_snapshot()
			if config['host'] != current['host'] or config['port'] != current['port']:
				raise ValueError("host and port changes require a server restart")
		except (ValueError, TypeError) as e:
			time.sleep(.25)
			try:
				with open(self.config_path, 'rb') as config_file:
					latest_content = config_file.read()
			except OSError:
				latest_content = content
			if latest_content != content:
				return
			self.log.print(f"Configuration reload rejected: {e}. Restoring the last known-good config.", 2)
			try:
				self._restore_last_good_config()
			except OSError as restore_error:
				self.log.print(f"Unable to restore configuration file: {restore_error}", 3)
			return

		self._apply_config(config, ip_whitelist, whitelist_enabled)
		self._last_good_content = content
		self.log.print("Configuration reloaded successfully.", 1)

	def _watch_config(self):
		while not self._config_watch_stop.wait(10):
			self._reload_config_once()

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

	def _get_client_ip(self, client):
		address = client.get("address")
		if not isinstance(address, (tuple, list)) or not address:
			return None
		return str(address[0])

	def _is_ip_allowed(self, client):
		config = self._config_snapshot()
		if not config['ip_whitelist_enabled']:
			return True

		client_ip = self._get_client_ip(client)
		if client_ip is None:
			return False
		try:
			ip = ipaddress.ip_address(client_ip)
			if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
				ip = ip.ipv4_mapped
			return any(ip.version == network.version and ip in network for network in config['ip_whitelist'])
		except ValueError:
			return False

	def _new_client(self, client, server):
		client_ip = self._get_client_ip(client)
		if not self._is_ip_allowed(client):
			self.log.print(f"Rejected connection from non-whitelisted IP: {client_ip or 'unknown'}", 2)
			try:
				client["handler"].send_close(1008, b'IP not allowed')
				server._terminate_client_handler(client["handler"])
			except Exception as e:
				self.log.print(f"Unable to reject client connection: {e}", 3)
			if client in server.clients:
				server.clients.remove(client)

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
		config = self._config_snapshot()
		if data_type == 'cmd':
			try:
				exec_conf = config['conf'].get('exec')
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
					encoding=config['log_encoding'],
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

				deadline = time.monotonic() + config['timeout_seconds']
				reason = None
				while p.poll() is None:
					self._drain_output(server, client, output_queue)
					if cancelled.is_set():
						reason = "cancelled"
						self._terminate_process(p)
						break
					if time.monotonic() >= deadline:
						reason = "timeout"
						self.log.print(f"Task timed out after {config['timeout_seconds']} seconds.", 2)
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
					self._close_and_report(server, client, f"Task timed out after {config['timeout_seconds']} seconds.", f"Task timed out after {config['timeout_seconds']} seconds.", loglv=2)
					return -1
				self._close_and_report(server, client, f"Task completed.", 'Done.', loglv=1)
				return 0
			except Exception as e:
				self._close_and_report(server, client, f"**Error occured. Detail: \n-----\n{e}", 'Server error.')
				return -1
		elif data_type == 'hello':
			exec_conf = config['conf'].get('exec')
			command_names = [str(k) for k in exec_conf.keys()] if isinstance(exec_conf, dict) else []
			self._close_and_report(server, client, f"New commander online. CMD menu provided.", json.dumps(command_names), 1)
		else:
			self._close_and_report(server, client, f"Unknown message type: {data_type}", 'Unknown message type.')
		
	def start(self):
		config = self._config_snapshot()
		server = WebsocketServer(port=config['port'], host=config['host'])
		server.set_fn_new_client(self._new_client)
		server.set_fn_message_received(self._msgReceived)
		server.set_fn_client_left(self._client_left)
		self.log.print(f"Listening on {self.host}:{self.port}...", 1)
		config_watcher = threading.Thread(target=self._watch_config, daemon=True)
		config_watcher.start()
		try:
			server.run_forever()
		finally:
			self._config_watch_stop.set()
			config_watcher.join(timeout=1)
