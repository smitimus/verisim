"""
Declarative validation for a Verisim generator config.yaml (t_6081478a).

Before this, every product's config.py hand-parsed YAML with chains of
``if '<key>' in block: setattr(...)``. That shape cannot notice a key it does
not know: a typo is not an error, it is a silently dropped line, and the
dataclass default quietly wins for the rest of the process's life. Measured on
the shipped grocery/config.yaml, ``scenarios.promotion.discount_pct`` looked
configured and did nothing — the loader only ever read ``labor_multiplier``
out of that block.

This module replaces the chains with a schema table plus one generic pass:
every key the document has must be a key the product declares, every declared
key's value must coerce to the annotation on the dataclass field it maps to,
and anything left over is reported by dotted path with a "did you mean".

Design notes, all four load-bearing:

1. **Types come from the dataclasses, never from a second table.** A rule says
   only "yaml path X maps to attribute path Y"; the expected type is read off
   the field annotation when the value is coerced. A hand-written type column
   would be one more place to forget, and would drift the first time somebody
   widened a field.

2. **A plain rule owns its whole subtree.** The walk stops descending the moment
   a node matches a rule, so ``products.departments`` (a list of nested dicts)
   and ``volumes.day_of_week_multipliers`` (open day names, float values) need
   no special cases — the coercer validates the inside of them.

3. **A fan-out rule owns one node's sub-keys.** ``min``/``max`` pairs are one
   YAML node feeding two attributes, which a flat rule cannot express. Handled
   here rather than by a second pass in each product, so the two cannot desync:
   a fan-out rule is part of the same table and is walked by the same code.

4. **Problems are COLLECTED, not raised at the first one.** A file with three
   typos should cost one edit cycle, not three.

No new dependencies — difflib is stdlib and the coercer is a few branches.

Note on copies: each product's generator image is built with
``context: ./generator``, so a module at the repo root cannot reach the running
generator. The canonical copy therefore lives at ``base/config_schema.py`` and
is mirrored into each product's ``generator/`` directory;
grocery/generator/tests/test_config_schema_parity.py asserts the copies are
byte-identical, so they cannot drift.
"""
import difflib
import typing
from dataclasses import fields as dataclass_fields


class ConfigValidationError(ValueError):
    """config.yaml does not match the schema its product declares.

    Carries every problem found, not just the first, so one edit pass clears
    the whole file.
    """

    def __init__(self, product, path, errors):
        self.product = product
        self.path = path
        self.errors = list(errors)
        super().__init__(self._render())

    def _render(self):
        plural = 'problem' if len(self.errors) == 1 else 'problems'
        lines = [f'{self.product} config is invalid ({self.path}): '
                 f'{len(self.errors)} {plural}', '']
        for i, err in enumerate(self.errors, 1):
            lines.append(f'  {i}. {err}')
        lines += ['', 'Fix the key(s) above in config.yaml. Every other key was accepted; '
                      'nothing was applied.']
        return '\n'.join(lines)


def validate_and_apply(cfg, data, schema, unused=None, path_hint='config.yaml',
                       product='config'):
    """Validate `data` against `schema` and apply it to `cfg`.

    `schema` is a sequence of rules, each a tuple of two to four elements:

        (yaml_path, attr_path)
            one YAML value into one dataclass attribute. The rule owns its whole
            subtree, so `products.departments` needs no inner schema.

        (yaml_path, attr_prefix, {yaml_subkey: attr_subkey, ...})
            a fan-out: one YAML node whose sub-keys each feed their own
            attribute, e.g. ``locations.store_employees_per_location.min`` ->
            ``locations.store_employees_per_location_min``. `attr_prefix` is the
            ATTRIBUTE CONTAINER the leaves hang off (``('locations',)``), not
            the yaml path — the two spellings differ, and conflating them is
            what made the first draft of this module resolve types against a
            field that does not exist.

        (..., validator)
            an optional final element, a callable taking the coerced value and
            raising ValueError with a usable message when it is wrong for
            something the type cannot express. It lives in the same tuple as the
            key it guards so the two cannot drift apart.

    `unused` maps a yaml_path the product deliberately accepts but does not read
    to the reason it is inert. Those come back as warnings instead of raising, so
    a shipped-but-inert key is never silent but never blocks a boot either.

    Raises :class:`ConfigValidationError` listing EVERY problem; cfg is left
    untouched in that case, so a rejected document never half-applies.
    """
    if not data:
        return []

    unused = {tuple(k): v for k, v in (unused or {}).items()}
    rules = _normalize(schema)
    errors = []
    assignments = []
    warnings = _collect(data, (), rules, unused, assignments, errors)

    # Coerce every candidate value; a bad type is a normal error, not a crash.
    coerced = []
    for attr_path, value, yaml_path, validator in assignments:
        try:
            annotation = _resolve_type(cfg, attr_path)
            value = _coerce(value, annotation, '.'.join(yaml_path))
            if validator is not None:
                try:
                    validator(value)
                except ValueError as exc:
                    raise TypeError(str(exc))
            coerced.append((attr_path, value, yaml_path))
        except TypeError as exc:
            errors.append(f'{".".join(yaml_path)} — {exc}')

    if errors:
        raise ConfigValidationError(product, path_hint, errors)

    for attr_path, value, _ in coerced:
        _set_path(cfg, attr_path, value)
    return warnings


# ---------------------------------------------------------------------------
# reusable validators
# ---------------------------------------------------------------------------

def length(n):
    """A sequence that must hold exactly n entries.

    Used for the 24 hourly weights, where anything else makes the scenario
    engine index off the end of the list — or, worse, silently weight the day
    by the first few hours only.
    """

    def _check(value):
        if len(value) != n:
            raise ValueError(f'expected exactly {n} entries, got {len(value)}')
        return value

    return _check


def sums_to(total, places=6):
    """A sequence of numbers that must add up to `total`.

    The scenario engine scales volume by weight x 24, which assumes the 24
    hourly weights sum to 1.0. Anything else inflates or deflates a whole day
    of volume by the same factor, everywhere, forever.
    """

    def _check(value):
        got = sum(value)
        if abs(got - total) > places:
            raise ValueError(
                f'expected the entries to sum to {total}, got {got:.6f} '
                f'(volume scales by entry x 24, so the sum must be 1.0)')
        return value

    return _check


# ---------------------------------------------------------------------------
# the walk
# ---------------------------------------------------------------------------

def _normalize(schema):
    """(yaml_path, (attr_path, subkey_map_or_None, validator_or_None))."""
    out = {}
    for rule in schema:
        yaml_path = tuple(rule[0])
        attr_path = tuple(rule[1])
        subs = rule[2] if len(rule) > 2 and rule[2] else None
        validator = rule[3] if len(rule) > 3 else None
        out[yaml_path] = (attr_path,
                          {str(k): str(v) for k, v in subs.items()} if subs else None,
                          validator)
    return out


def _collect(node, prefix, rules, unused, assignments, errors):
    """Walk the document, matching nodes against the rules.

    A plain rule consumes its whole subtree and is not descended into. A fan-out
    rule descends one level, because its node is a container of its own keys. A
    node that is merely a step on the way to a rule (``locations``) is descended
    into. Anything else is unknown.
    """
    warnings = []
    if not isinstance(node, dict):
        return warnings
    containers = {y[:i] for y in rules for i in range(1, len(y))}

    for key, value in node.items():
        path = prefix + (str(key),)
        if path in rules:
            attr_path, subs, validator = rules[path]
            if subs is None:
                assignments.append((attr_path, value, path, validator))
            else:
                warnings += _collect_fanout(value, path, attr_path, subs, assignments,
                                            errors)
        elif path in unused:
            warnings.append(f'{".".join(path)} — {unused[path]}')
        elif path in containers:
            warnings += _collect(value, path, rules, unused, assignments, errors)
        else:
            errors.append(_unknown_message(path, value, rules))
    return warnings


def _collect_fanout(node, path, attr_prefix, subs, assignments, errors):
    """Apply a min/max-style fan-out: one YAML node, several attributes.

    A `min` above its `max` is caught here rather than deep in the generator:
    an inverted range produces a Config that looks fine and then draws from the
    wrong end of it (a negative per-tick count, or an employee headcount loop
    that never runs), which is exactly the kind of failure a config error should
    not be able to cause.
    """
    warnings = []
    dotted = '.'.join(path)
    if not isinstance(node, dict):
        errors.append(f'{dotted} — expected a mapping with '
                      f'{"/".join(sorted(subs))} keys, got {_typename(node)} {node!r}')
        return warnings

    for key, value in node.items():
        if str(key) not in subs:
            errors.append(_unknown_message(path + (str(key),), value,
                                          {path + (str(k),): (attr_prefix, None, None)
                                           for k in subs}))
            continue
        assignments.append((attr_prefix + (subs[str(key)],), value, path + (str(key),),
                            None))

    lo, hi = node.get('min'), node.get('max')
    if isinstance(lo, (int, float)) and isinstance(hi, (int, float)) and not (
            isinstance(lo, bool) or isinstance(hi, bool)) and lo > hi:
        errors.append(f'{dotted} — min ({lo}) is greater than max ({hi}); '
                      f'the generator would draw from an empty range')
    return warnings


def _unknown_message(path, value, rules):
    """Name the nearest keys we DO read, so the fix is obvious from the message."""
    dotted = '.'.join(path)
    shown = f'{{{len(value)} keys}}' if isinstance(value, dict) else repr(value)
    msg = (f'unknown key "{dotted}" ({_typename(value)} {shown}) — nothing reads it, '
           f'so the default silently applies for the whole run.')

    siblings = sorted(
        '.'.join(y) for y in rules if y[:-1] == path[:-1] and rules[y][1] is None)
    if siblings:
        # Offer the closest sibling spelling, falling back to the attribute name
        # it maps to: the reader usually knows one of the two, not both.
        near = difflib.get_close_matches(dotted, siblings, n=2, cutoff=0.6)
        if not near:
            near = difflib.get_close_matches(
                path[-1],
                sorted({a[-1] for y, (a, _s, _v) in rules.items()
                        if y[:-1] == path[:-1]}),
                n=1, cutoff=0.5)
            near = ['.'.join(path[:-1] + (n,)) for n in near]
        if near:
            msg += ' Did you mean ' + ' or '.join(f'"{n}"' for n in near) + '?'
        else:
            msg += (' Keys read in this block: '
                    + ', '.join(f'"{s}"' for s in siblings[:8]) + '.')
    return msg


# ---------------------------------------------------------------------------
# coercion — driven by the dataclass annotation
# ---------------------------------------------------------------------------

def _resolve_type(cfg, attr_path):
    """The annotation on the dataclass field an attr_path names."""
    obj = cfg
    for name in attr_path[:-1]:
        obj = getattr(obj, name)
    for f in dataclass_fields(obj):
        if f.name == attr_path[-1]:
            return f.type
    raise KeyError(f'attr_path {".".join(attr_path)} does not name a dataclass field')


def _coerce(value, annotation, path):
    """Coerce a YAML value to `annotation`, or raise TypeError with a usable message.

    Annotation handling is deliberately narrow: it covers exactly what the three
    config dataclasses use (scalars, List[x], Dict[str, x]) and passes anything
    else through untouched rather than guessing.
    """
    if annotation is bool:
        if isinstance(value, bool):
            return value
        raise TypeError(f'expected a boolean (true/false), got {_typename(value)} {value!r}')

    if annotation is int:
        if isinstance(value, bool):
            raise TypeError(f'expected an integer, got the boolean {value!r}')
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        raise TypeError(f'expected an integer, got {_typename(value)} {value!r}')

    if annotation is float:
        if isinstance(value, bool):
            raise TypeError(f'expected a number, got the boolean {value!r}')
        if isinstance(value, (int, float)):
            return float(value)
        raise TypeError(f'expected a number, got {_typename(value)} {value!r}')

    if annotation is str:
        if isinstance(value, str):
            return value
        raise TypeError(f'expected a string, got {_typename(value)} {value!r}')

    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)

    if origin is list or origin is typing.List:
        if not isinstance(value, list):
            raise TypeError(f'expected a list, got {_typename(value)} {value!r}')
        if not args:
            return list(value)
        inner = args[0]
        # `List[Dict]` with no parameters is an open record list (the department
        # and category trees); its inner shape is data, so copy rather than
        # police keys this layer knows nothing about.
        if inner is dict or inner is typing.Dict:
            return [dict(v) if isinstance(v, dict) else v for v in value]
        return [_coerce(v, inner, f'{path}[{i}]') for i, v in enumerate(value)]

    if origin is dict or origin is typing.Dict:
        if not isinstance(value, dict):
            raise TypeError(f'expected a mapping, got {_typename(value)} {value!r}')
        if len(args) != 2:
            return dict(value)
        return {str(k): _coerce(v, args[1], f'{path}.{k}') for k, v in value.items()}

    return value


# ---------------------------------------------------------------------------
# small path helpers
# ---------------------------------------------------------------------------

def _set_path(cfg, attr_path, value):
    cur = cfg
    for name in attr_path[:-1]:
        cur = getattr(cur, name)
    setattr(cur, attr_path[-1], value)


def _typename(value):
    return type(value).__name__
