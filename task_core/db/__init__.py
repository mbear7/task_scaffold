# -*- coding: utf-8 -*-
# Deliberately empty, matching task_core/resources/__init__.py:
# task_core/__init__.py imports directly from task_core.db.publish and
# task_core.db.copy rather than through this file, so there is no re-export
# logic here to keep in sync.
#
# The four modules form one subsystem with its own layering, which the
# architecture diagram states and tests/test_docs.py enforces:
#
#     publish -> insert
#     publish -> copy -> values
#     publish ---------> values
