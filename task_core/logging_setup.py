"""Level 1 leaf: zero task_core dependencies. Only ever called by end users
(ops_task.py-style task files), never imported by another task_core
submodule -- kept out of __init__.py itself so the facade stays pure
re-exports, with no non-reexport logic of its own."""

import logging


def setup_logging(task_name, level=logging.INFO, smb_level=logging.WARNING):

    # set level=logging.CRITICAL to silence routine logs during local runs.
    # smb_level defaults to WARNING to keep smbprotocol/smbclient's own
    # (very verbose) protocol-level logging out of routine task logs; pass
    # logging.DEBUG here to see actual SMB session/auth/tree-connect
    # traffic when diagnosing DFS access issues.
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    logging.getLogger('smbprotocol').setLevel(smb_level)
    logging.getLogger('smbclient').setLevel(smb_level)

    return logging.getLogger(task_name)
