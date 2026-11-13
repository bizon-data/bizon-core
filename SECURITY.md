# Security

Security fixes target the latest published version of Bizon. Upgrade to the latest
release and use patched dependencies, including the optional extras you enable.

Report suspected vulnerabilities privately through
[GitHub's vulnerability reporting form](https://github.com/bizon-data/bizon-core/security/advisories/new).
Include the affected version, the configuration needed to reproduce the issue,
and a minimal reproduction without real credentials or customer data.

Pipeline YAML and Python transforms are trusted executable configuration. Do not
run pipeline definitions supplied by untrusted users with production credentials.
