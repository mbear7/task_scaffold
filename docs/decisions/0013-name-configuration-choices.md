# 0013 — Name configuration choices

Status: accepted.

## Principle

Configuration choices are named. Natural values may be positional. Stable
result contracts remain unchanged. Internal records optimize for clarity and
measured performance rather than blanket stylistic consistency.

## Context

Author-facing configuration dataclasses accumulate fields over time. When
positional construction is supported, declaration order becomes part of the
public API. New fields must then be appended instead of placed beside related
settings, creating a positional append treadmill.

Long sequences of positional booleans, numeric limits, optional values and
policy objects are also difficult to review and easy to misconfigure.
`PipelineSpec` had reached this state: comments required every new field to be
appended solely to preserve the meaning of old positional calls, even though
all known task files already construct it with keywords.

Not every dataclass has the same role. Configuration objects, natural value
objects, stable result contracts and internal transport records need different
constructor semantics.

## Decision

Dataclasses are classified by role.

### 1. Author-facing configurations

Task-authored configuration objects use keyword-only constructors.

The initial set is:

- `PipelineSpec`;
- `PublisherConfig`;
- `PublicationLockPolicy`;
- `CopyLoadPolicy`;
- `SourceChangeCheckConfig`;
- `ResourceEnvironment`;
- `IdentifierPolicy`.

These classes use `kw_only=True` unless a documented mixed constructor is more
appropriate. This lets fields be grouped logically and extended without making
positional field order part of the API.

### 2. Natural and mixed value objects

Objects with a small, obvious value identity may retain positional
construction. For a mixed object, essential identity fields remain positional
while optional policy choices are keyword-only.

`OutputColumn` therefore keeps `name` and `type` positional, while `nullable`
is keyword-only:

```python
OutputColumn('customer_id', sa.BigInteger(), nullable=False)
```

`ResourceSpec` keeps `loader` positional, while `tracker` is keyword-only:

```python
ResourceSpec(load_source, tracker=True)
```

### 3. Stable result contracts

Framework result objects, including `RunResult` and `DbRunResult`, retain their
existing constructor and field behavior.

This decision does not change their field order, attributes, equality
semantics, serialization behavior or external construction contract.
Dataclasses do not imply tuple unpacking; the preserved contract is their
named fields and existing constructor behavior.

### 4. Internal records

Internal transport and domain records may remain positional where field order
is natural and concise construction improves readability.

The use of `slots=True` is a separate implementation decision. It may be
applied where object volume or profiling justifies it and where dynamic
attributes, inheritance and weak-reference behavior are not required. This ADR
does not mandate slots.

## Consequences

- Configuration calls are explicit and easier to review.
- Adjacent booleans, limits and optional policies cannot be confused
  positionally.
- Configuration fields may be reordered and grouped logically without changing
  constructor semantics.
- Existing positional calls to converted configuration classes fail with
  `TypeError` and must be rewritten using keywords.
- No compatibility shim is provided.
- Natural value objects remain concise where positional construction is clear.
- `RunResult`, `DbRunResult` and other excluded result contracts remain
  unchanged.
- `slots=True` remains an independent, evidence-driven optimization choice.

Keyword-only construction removes positional-order coupling. It does not make
all future changes backward compatible: renaming or removing a field remains a
public API change.

## Release impact

This is an intentional public constructor change and is released as 0.7.0.
Repository tasks and known external tasks were reviewed before the change and
already use keyword construction for `PipelineSpec`. Tests that previously
preserved positional meaning are replaced by tests that require positional
configuration to fail.

## Future rule

Before adding a dataclass, classify it as one of:

1. author-facing configuration;
2. natural or mixed value object;
3. stable result contract;
4. internal transport or domain record.

Constructor semantics follow that role rather than a universal dataclass style
rule.
