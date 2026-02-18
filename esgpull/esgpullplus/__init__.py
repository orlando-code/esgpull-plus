"""
ESGPullPlus - Enhanced functionality for esgpull package.

This module patches the base esgpull package to handle edge cases gracefully
and ensure async compatibility in both CLI and Jupyter notebook contexts.
"""

import logging

logger = logging.getLogger(__name__)


def _patch_sniffio():
    """
    Patch sniffio.current_async_library to default to ``"asyncio"`` when
    automatic detection fails.

    httpcore (used by httpx) calls ``sniffio.current_async_library()`` to
    decide which async backend to use.  Inside ``nest_asyncio``'s nested
    ``run_until_complete``, ``asyncio.current_task()`` can return ``None``
    (especially on Python >= 3.12), causing sniffio to raise
    ``AsyncLibraryNotFoundError``.

    Since esgpull always uses asyncio, defaulting to ``"asyncio"`` is safe.
    """
    try:
        import sniffio

        _original_sniffio = sniffio.current_async_library

        def _patched_sniffio():
            try:
                return _original_sniffio()
            except sniffio.AsyncLibraryNotFoundError:
                return "asyncio"

        sniffio.current_async_library = _patched_sniffio
    except ImportError:
        pass


def _patch_distribute_hits():
    """Patch _distribute_hits_impl to handle empty or zero-sum hits gracefully."""
    try:
        from esgpull import context

        original = context._distribute_hits_impl

        def _patched(hits, max_hits):
            if not hits:
                return []
            if sum(hits) == 0:
                return [0] * len(hits)
            return original(hits, max_hits)

        context._distribute_hits_impl = _patched
    except (ImportError, AttributeError):
        pass


def _patch_sync():
    """
    Patch esgpull's sync() wrapper to work in both CLI and notebook contexts.

    In a notebook (or any context with an already-running event loop),
    asyncio.run() fails and nest_asyncio causes "cannot create weak reference
    to 'NoneType'" errors (especially Python 3.12+).  The fix: run the coroutine
    in a **separate thread** with a fresh asyncio.run(), avoiding nested loops
    and weakref bugs.

    IMPORTANT: ``context.py`` does ``from esgpull.utils import sync`` at
    module level — we must patch both utils.sync and context.sync.
    """
    try:
        import asyncio
        import concurrent.futures
        from esgpull import utils, context

        def _use_thread_path() -> bool:
            """Detect when we must use the thread path to avoid weakref/loop issues."""
            _sys = __import__("sys")
            # Notebook/IPython: always use thread (nest_asyncio causes weakref errors)
            if "IPython" in _sys.modules or "ipykernel" in _sys.modules:
                return True
            # get_ipython() exists and returns something -> interactive shell / notebook
            try:
                get_ipython = __import__("IPython", fromlist=["get_ipython"]).get_ipython
                if get_ipython() is not None:
                    return True
            except (ImportError, AttributeError):
                pass
            # Check for running event loop (avoid asyncio.get_event_loop() - can trigger
            # weakref issues; use get_running_loop which only raises if no loop)
            try:
                asyncio.get_running_loop()
                return True  # Loop is running
            except RuntimeError:
                return False

        def robust_sync(coro, before_cb=None, after_cb=None):
            if before_cb is not None:
                before_cb()
            if _use_thread_path():
                # Notebook / IPython: run in a fresh thread to avoid nesting
                # and weakref/NoneType errors from nest_asyncio
                result_holder = []
                exc_holder = []

                def run_in_thread():
                    try:
                        result_holder.append(asyncio.run(coro))
                    except Exception as e:
                        exc_holder.append(e)

                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                    ex.submit(run_in_thread).result()

                if exc_holder:
                    raise exc_holder[0]
                result = result_holder[0]
            else:
                result = asyncio.run(coro)

            if after_cb is not None:
                after_cb()
            return result

        utils.sync = robust_sync
        context.sync = robust_sync
    except (ImportError, AttributeError):
        pass


# Apply all patches on import
_patch_sniffio()
_patch_distribute_hits()
_patch_sync()
