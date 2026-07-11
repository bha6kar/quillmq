# Security Policy

## Supported versions

QuillMQ is pre-1.0. Security fixes are applied to the latest released version.

## Reporting a vulnerability

Please do not open a public issue for security problems.

Report privately using GitHub's ["Report a vulnerability"](https://github.com/bha6kar/quillmq/security/advisories/new)
flow, or email bha6kar@gmail.com. Include a description, reproduction steps, and
the affected version. You can expect an acknowledgement within a few days.

## Scope and hardening notes

QuillMQ is a single-node broker intended for trusted networks. Be aware that in
the current release:

- TLS is optional and off by default. Without it, traffic (including the
  shared-secret auth token) is sent in clear text, so enable TLS with
  `--tls-cert`/`--tls-key` and a `quills://` URL, or run only on a private
  network.
- The auth token is a single shared secret, not per-client credentials.

Treat the broker as you would any internal service: enable TLS, and do not
expose the port to the public internet without additional network controls.
