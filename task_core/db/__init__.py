# Deliberately empty, matching task_core/resources/__init__.py:
# task_core/__init__.py imports directly from the modules that define what
# it re-exports, so there is no re-export logic here to keep in sync.
#
# Ten modules form one subsystem -- publication lifecycle, staging loaders,
# spool format and the schema/value kernel. Their order is stated in
# docs/architecture.md and enforced by
# tests/test_docs.py::test_the_db_subsystem_order_is_as_documented, so it is
# deliberately not duplicated here: a second copy is a second thing to go
# stale, and this one already had, still describing four modules and a
# three-arrow diagram after the split made it ten.
