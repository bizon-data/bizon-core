import os

from ..resolver import AbstractReferenceResolver, ReferenceResolutionError


class EnvResolver(AbstractReferenceResolver):
    """Resolves ``env://VAR_NAME`` references to environment variables.

    Unlike the legacy whole-value ``BIZON_ENV_`` prefix, ``env://`` also works inline,
    e.g. ``dsn: "postgres://u:${env://PG_PASSWORD}@host/db"``.
    """

    scheme = "env"

    def resolve(self, path: str) -> str:
        var_name = path.strip()
        if var_name not in os.environ:
            raise ReferenceResolutionError(f"environment variable '{var_name}' is not set")
        return os.environ[var_name]
