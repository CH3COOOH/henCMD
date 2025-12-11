import json
import time
import subprocess
from websocket_server import WebsocketServer
import azstd.out
import azstd.file.json

class WServer:
	def __init__(self, conf_path):
		self.conf = azstd.file.json.gracefulLoadJSON(conf_path)
		self.host = self.conf['host']
		self.port = self.conf['port']
		self.log = azstd.out.Log(show_level=self.conf['log_level'])
		self.log_encoding = 'ansi'
		if 'log_encoding' in self.conf.keys():
			self.log_encoding = self.conf['log_encoding']

	def _close_session(self, client):
		client["handler"].send_close(1000, b'')

	def _send_msg(self, server, client, msg):
		server.send_message(client, msg)

	def _close_and_report(self, server, client, show_console, show_client, loglv=3):
		self.log.print(show_console, loglv)
		self._send_msg(server, client, show_client)
		self._close_session(client)

	def _msgReceived(self, client, server, msg):
		try:
			d_msg = json.loads(msg)
			data_type = d_msg['type']
		except Exception as e:
			self._close_and_report(server, client, f"Unable to parse msg: {msg}. {e}", 'Data format error.')
			return -1
		if data_type == 'cmd':
			try:
				if self.conf['exec'] == None:
					self._close_and_report(server, client, 'No command defined.', 'Server config error.')
					return -1
				exec_name = self.conf['exec'][d_msg['param']]
				exec_head = exec_name.split(' ')
				exec_head.append(d_msg['data'])
				self.log.print(f"EXEC > {' '.join([exec_name, d_msg['data']])}", 2)
				p = subprocess.Popen(exec_head,
					shell=False,
					stdout=subprocess.PIPE,
					stderr=subprocess.STDOUT,
					text=True,
					encoding=self.log_encoding,
					errors="ignore",
					bufsize=4096)
				for line in p.stdout:
					self._send_msg(server, client, line.strip())
					time.sleep(.05)
				p.wait()
				self._close_and_report(server, client, f"Task completed.", 'Done.', loglv=1)
				return 0
			except Exception as e:
				self._close_and_report(server, client, f"**Error occured. Detail: \n-----\n{e}", 'Server error.')
				return -1
		elif data_type == 'hello':
			self._close_and_report(server, client, f"New commander online. CMD menu provided.", json.dumps([str(k) for k in self.conf['exec'].keys()]), 1)
		else:
			self._close_and_report(server, client, f"**Error occured. Detail: \n-----\n{e}", 'Server error.')
		
	def start(self):
		server = WebsocketServer(port=self.port, host=self.host)
		server.set_fn_message_received(self._msgReceived)
		self.log.print(f"Listening on {self.host}:{self.port}...", 1)
		server.run_forever()
