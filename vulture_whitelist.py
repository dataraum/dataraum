# Vulture whitelist — false positives that should not be flagged.
#
# These are variables/parameters required by framework callback signatures
# (SQLAlchemy event listeners, structlog processors) or code that vulture
# cannot statically determine is reachable.

# SQLAlchemy event.listens_for("connect") — callback + parameter required by framework
configure_sqlite  # noqa
connection_record  # noqa

# structlog processor/renderer callbacks require method_name parameter
method_name  # noqa

# Module-level __getattr__ for lazy imports — Python calls it automatically
__getattr__  # noqa

# Note: "unreachable code after 'while'" false positives (while True + break)
# are filtered via grep in the hook and CI scripts, not via this whitelist.
