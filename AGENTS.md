# Acquirium developer guidance

## Source comments and docstrings

Use Redis's source as a model for explaining how the implementation works close to the code. [Redis dictionary source](https://github.com/redis/redis/blob/unstable/src/dict.c) · [Redis networking source](https://github.com/redis/redis/blob/unstable/src/networking.c)

- Give complex modules a short overview of their responsibilities, the data they own, and how work moves through them.
- Write docstrings for public entry points and non-obvious helpers. State what callers get and explain any preconditions, side effects, ordering guarantees, or failure behavior they need to know.
- Put inline comments next to the decisions they explain. Document why a lock spans a block, when a write becomes visible, or what a retry must preserve.
- Use the codebase's terminology and focus each comment on what a reader needs to understand. Update nearby comments and docstrings when the behavior changes.
