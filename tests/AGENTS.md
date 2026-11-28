# Development note on writing tests

Write tests so a reviewer can follow the scenario and see what behavior each
assertion covers. Name the test for the behavior it verifies. Keep the inputs,
operations, and expected results visible in the test body.

## Tell the story in order

- Use short comments at meaningful steps. Explain what is being set up, what
  action takes place, and what the next check proves. For a workflow test, tell
  the story in the order a user would follow it.
- Use fixtures for shared infrastructure, such as a temporary server. Keep
  the setup that gives the scenario its meaning close to the assertions.
- Use a small helper for a genuinely repeated operation. Give it a descriptive
  name and a short docstring that explains what it does or returns. Keep it
  inside the test when only that test needs it. Make important options visible
  where the operation runs, and explain waits for asynchronous work.

## Consolidate overlapping coverage

Before writing a test, look for the scenario in both the unit and integration
suites. Extend the existing test when it already exercises the relevant code.
Repeat a workflow across layers only when each test verifies a distinct
contract, such as transaction rollback and HTTP error reporting.

Combine tests when they repeat the same workflow and check related outcomes.
Keep each behavior and regression visible as an assertion or a clearly named
parameter case. Use parameterization for cases that differ only in their inputs
and expected results. Keep separate tests when the actions or failure modes
tell different stories.

The [experiment metadata deletion test](unit/test_experiment_metadata.py) is
the model. It declares a Study and two variables, records a baseline and a
trial, links the baseline to a plant stream, and checks what each deletion
removes. Its local helper explains what it queries and why it waits for the
graph to catch up.
