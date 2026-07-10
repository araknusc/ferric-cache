# TLS certificates

This directory intentionally ships **without** certificates. Private keys must
never be committed — `.gitignore` excludes `*.key`, `*.crt`, and `*.pem` here.

Generate a self-signed cert/key for local testing:

```bash
# from cache/
./generate_certs.sh      # Linux/macOS/Git Bash
generate_certs.bat       # Windows cmd.exe
```

This writes `server.key` and `server.crt` into this directory. Point
`tls_config.json` (`certFile` / `keyFile`) at them and start the server with
`--config tls_config.json`.

For production, use certificates issued by a trusted CA rather than self-signed.
