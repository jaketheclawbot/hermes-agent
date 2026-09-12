"""
Hermes Gateway - Multi-platform messaging integration.

This module provides a unified gateway for connecting the Hermes agent
to various messaging platforms (Telegram, Discord, WhatsApp, Weixin, and more) with:
- Session management (persistent conversations with reset policies)
- Dynamic context injection (agent knows where messages come from)
- Delivery routing (cron job outputs to appropriate channels)
- Platform-specific toolsets (different capabilities per platform)
"""

from importlib import import_module

__all__ = [
    # Config
    "GatewayConfig",
    "PlatformConfig", 
    "HomeChannel",
    "load_gateway_config",
    # Session
    "SessionContext",
    "SessionStore",
    "SessionResetPolicy",
    "build_session_context_prompt",
    # Delivery
    "DeliveryRouter",
    "DeliveryTarget",
]

_EXPORT_MODULES = {
    "GatewayConfig": "config",
    "PlatformConfig": "config",
    "HomeChannel": "config",
    "load_gateway_config": "config",
    "SessionContext": "session",
    "SessionStore": "session",
    "SessionResetPolicy": "session",
    "build_session_context_prompt": "session",
    "DeliveryRouter": "delivery",
    "DeliveryTarget": "delivery",
}


def __getattr__(name):
    """Load public gateway helpers only when callers request them.

    Importing a lightweight submodule such as ``gateway.session_context`` or
    ``gateway.status`` must not initialize configuration, session storage, and
    every delivery adapter.  Those imports sit on async dispatch's synchronous
    persistence path, where eager package initialization made a genuinely
    background dispatch take several seconds before its worker was submitted.
    """
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f".{module_name}", __name__), name)
    globals()[name] = value
    return value
