import sys
import os

if __name__ == '__main__':
	if len(sys.argv) != 2:
		print("Usage: python main.py <config.json>", file=sys.stderr)
		sys.exit(1)

	fpath_config = sys.argv[1]
	if not os.path.isfile(fpath_config):
		print(f"Configuration file not found: {fpath_config}", file=sys.stderr)
		sys.exit(1)

	try:
		import wsrv
		s = wsrv.WServer(fpath_config)
		s.start()
	except (OSError, ValueError, KeyError, TypeError, ImportError) as e:
		print(f"Startup failed: {e}", file=sys.stderr)
		sys.exit(1)