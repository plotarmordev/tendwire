# Black-box ACP adapter compatibility probe

Tendwire treats an ACP adapter as a separately installed executable. The
adapter is not vendored, imported, rebased, or coupled to a repository layout.
Run the compatibility probe after installing or upgrading any adapter and
before promoting that executable into the Tendwire runtime:

```console
python -m tendwire.backends.acp_probe -- /absolute/path/to/adapter adapter-arg
```

The command starts the supplied argv directly without a shell, negotiates ACP
v1 capabilities on a fresh process, closes the process, prints one bounded JSON
object, and exits zero only when negotiation and shutdown succeed. Use
`--timeout`, `--close-timeout`, and an absolute `--cwd` before the `--` marker
when needed. Probe timeouts have hard upper bounds.

The output is intentionally narrow. It contains fixed boolean capabilities,
bounded counts for authentication methods and unknown capability extensions, a
process-reaped flag, and a fixed failure category. It never contains:

- adapter argv or executable paths;
- working directories or environment values;
- stderr, exception text, or raw JSON-RPC payloads;
- agent-provided names, versions, extension names, or extension values;
- session IDs, messages, thoughts, tool data, plans, or authentication values.

Example successful shape:

```json
{
  "authentication": {"method_count": 0, "method_count_capped": false},
  "capabilities": {
    "additional_directories": true,
    "auth_logout": false,
    "mcp_http": false,
    "mcp_sse": false,
    "prompt_audio": false,
    "prompt_embedded_context": false,
    "prompt_image": false,
    "session_cancel": true,
    "session_close": true,
    "session_delete": true,
    "session_list": true,
    "session_load": true,
    "session_new": true,
    "session_prompt": true,
    "session_resume": true,
    "session_update": true
  },
  "compatible": true,
  "extensions": {"capability_count": 0, "capability_count_capped": false},
  "failure": null,
  "process_reaped": true,
  "protocol_version": 1,
  "schema_version": 1
}
```

An incompatible result exits with status 1 and reports only one of these stable
categories: `invalid_configuration`, `launch_failed`, `timeout`,
`protocol_version`, `protocol_error`, `transport_error`, `shutdown_failed`, or
`internal_error`. Keep the previously proven executable available for rollback;
the probe validates initialization and capability negotiation, not account
authentication or a stateful agent session.
