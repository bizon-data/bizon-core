"""Scheme-dispatched reference resolution for Bizon config.

Resolves URI-style references in a raw config dict *before* Pydantic validation, so
connectors never see a reference and need no changes. Two forms share one grammar
``<scheme>://<path>``:

- Whole value: ``token: gsm://notion-api-token`` (the whole field is one reference).
- Inline:      ``dsn: "postgres://u:${gsm://db-pw}@host/db"`` (embedded, multiple allowed).

Built-in schemes: ``gsm`` (Google Secret Manager) and ``env`` (environment variables).
A whole-value string whose scheme is *not* a known scheme is left untouched, so plain URIs
like ``postgresql://...`` or ``https://...`` are never mistaken for references.
"""

import re
from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Optional, Tuple

# Whole-value reference, e.g. "gsm://my-secret/versions/3". Scheme is lowercase/digits/_.
_WHOLE_RE = re.compile(r"^([a-z0-9_]+)://(.+)$", re.DOTALL)
# Strict inline token, e.g. "${gsm://my-secret}".
_INLINE_RE = re.compile(r"^\$\{\s*([a-z0-9_]+)://([^}]+?)\s*\}$")
# Broad inline token used to locate every "${...}" so malformed ones can be flagged.
_BROAD_INLINE_RE = re.compile(r"\$\{[^}]*\}")


class ReferenceResolutionError(Exception):
    """Raised when a config reference cannot be resolved."""


class AbstractReferenceResolver(ABC):
    """Resolves the ``<path>`` of a single ``<scheme>://<path>`` reference to a value."""

    scheme: str

    @abstractmethod
    def resolve(self, path: str) -> str: ...


def _build_env_resolver(settings: dict) -> AbstractReferenceResolver:
    from .adapters.env import EnvResolver

    return EnvResolver()


def _build_gsm_resolver(settings: dict) -> AbstractReferenceResolver:
    from .adapters.gcp.gsm import GSMResolver

    return GSMResolver(project_id=settings.get("project_id"))


# scheme -> lazy factory. Adding a provider (e.g. "awssm") is one entry + one adapter file.
_SCHEME_FACTORIES: Dict[str, Callable[[dict], AbstractReferenceResolver]] = {
    "env": _build_env_resolver,
    "gsm": _build_gsm_resolver,
}


def is_known_scheme(scheme: str) -> bool:
    return scheme in _SCHEME_FACTORIES


def known_schemes() -> List[str]:
    return sorted(_SCHEME_FACTORIES)


class ResolverRegistry:
    """Builds resolvers lazily per scheme and caches resolved values for the run.

    A resolver (and its dependency import / client) is only created the first time its
    scheme actually appears, so ``gsm://`` configs are zero-cost when unused.
    """

    def __init__(self, settings: Optional[dict] = None):
        self._settings = settings or {}
        self._resolvers: Dict[str, AbstractReferenceResolver] = {}
        self._cache: Dict[str, str] = {}

    def _get_resolver(self, scheme: str) -> AbstractReferenceResolver:
        if scheme not in self._resolvers:
            if scheme not in _SCHEME_FACTORIES:
                raise ReferenceResolutionError(
                    f"Unknown reference scheme '{scheme}'. Known schemes: {known_schemes()}."
                )
            self._resolvers[scheme] = _SCHEME_FACTORIES[scheme](self._settings.get(scheme) or {})
        return self._resolvers[scheme]

    def resolve(self, scheme: str, path: str) -> str:
        reference = f"{scheme}://{path}"
        if reference not in self._cache:
            self._cache[reference] = self._get_resolver(scheme).resolve(path)
        return self._cache[reference]

    def resolve_reference(self, reference: str) -> str:
        match = _WHOLE_RE.match(reference.strip())
        if not match:
            raise ReferenceResolutionError(
                f"'{reference}' is not a valid reference. Expected '<scheme>://<path>', e.g. 'gsm://my-secret'."
            )
        return self.resolve(match.group(1), match.group(2))


def _resolve_string(value: str, registry: ResolverRegistry) -> str:
    # No inline tokens: the whole string may itself be a reference.
    if "${" not in value:
        match = _WHOLE_RE.match(value.strip())
        if match and is_known_scheme(match.group(1)):
            return registry.resolve(match.group(1), match.group(2))
        return value

    # Inline tokens: substitute every "${...}", flagging malformed/unknown ones.
    def _replace(token_match: "re.Match") -> str:
        token = token_match.group(0)
        inner = _INLINE_RE.match(token)
        if not inner:
            raise ReferenceResolutionError(
                f"Malformed reference token '{token}'. Expected '${{<scheme>://<path>}}', e.g. '${{gsm://my-secret}}'."
            )
        scheme, path = inner.group(1), inner.group(2).strip()
        if not is_known_scheme(scheme):
            raise ReferenceResolutionError(
                f"Unknown reference scheme '{scheme}' in '{token}'. Known schemes: {known_schemes()}."
            )
        return registry.resolve(scheme, path)

    return _BROAD_INLINE_RE.sub(_replace, value)


def _resolve_value(value, registry: ResolverRegistry, path: str):
    if isinstance(value, dict):
        return {k: _resolve_value(v, registry, f"{path}.{k}" if path else k) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_value(v, registry, f"{path}[{i}]") for i, v in enumerate(value)]
    if isinstance(value, str):
        try:
            return _resolve_string(value, registry)
        except ReferenceResolutionError as error:
            raise ReferenceResolutionError(f"Failed to resolve reference at '{path}': {error}") from error
    return value


def resolve_references_in_config(config: dict, settings: Optional[dict] = None) -> dict:
    """Return a copy of ``config`` with every ``gsm://`` / ``env://`` reference resolved.

    Provider defaults are read from the top-level ``secrets:`` block unless ``settings`` is
    passed explicitly.
    """
    if settings is None:
        settings = config.get("secrets") or {}
    registry = ResolverRegistry(settings=settings)
    return _resolve_value(config, registry, "")


def _references_in_string(value: str) -> List[str]:
    if "${" in value:
        refs = []
        for token_match in _BROAD_INLINE_RE.finditer(value):
            inner = _INLINE_RE.match(token_match.group(0))
            if inner:
                refs.append(f"{inner.group(1)}://{inner.group(2).strip()}")
        return refs
    match = _WHOLE_RE.match(value.strip())
    if match and is_known_scheme(match.group(1)):
        return [value.strip()]
    return []


def collect_references_in_config(config: dict) -> List[Tuple[str, str]]:
    """Return ``(config_path, reference)`` for every reference, without resolving.

    Used by ``bizon secrets check`` to dry-run/validate references.
    """
    found: List[Tuple[str, str]] = []

    def _walk(value, path: str) -> None:
        if isinstance(value, dict):
            for key, sub in value.items():
                _walk(sub, f"{path}.{key}" if path else key)
        elif isinstance(value, list):
            for index, sub in enumerate(value):
                _walk(sub, f"{path}[{index}]")
        elif isinstance(value, str):
            for reference in _references_in_string(value):
                found.append((path, reference))

    _walk(config, "")
    return found


def resolve_reference(reference: str, settings: Optional[dict] = None) -> str:
    """Resolve a single reference (e.g. at connector runtime). See module docstring."""
    return ResolverRegistry(settings=settings).resolve_reference(reference)
