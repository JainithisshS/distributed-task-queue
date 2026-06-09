"""Queue package for distributed task queue engine."""

# Re-export built-in queue module items to avoid import conflicts
import sys
from builtins import __import__ as builtin_import

# Temporarily replace this module in sys.modules with built-in queue
# so that external imports work correctly
_builtin_queue = builtin_import('queue')

# Export commonly used items from the built-in queue
Empty = _builtin_queue.Empty
Full = _builtin_queue.Full
LifoQueue = _builtin_queue.LifoQueue
Queue = _builtin_queue.Queue

