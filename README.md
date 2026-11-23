# henCMD
Run CMD via Websocket.

## Configuration hot reload

The server checks the config file every 10 seconds. Valid changes to commands,
IP whitelist, timeout, log encoding, and log level take effect without restarting.
If the updated file is invalid, the server restores the last known-good contents
to the config file. Changes to `host` or `port` require a server restart and are
reverted if attempted while the server is running.
