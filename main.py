import sys
import wsrv

if __name__ == '__main__':
	fpath_config = sys.argv[1]
	s = wsrv.WServer(fpath_config)
	s.start()