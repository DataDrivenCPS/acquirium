---
title: Derived computation internals
---

Derived computation is now owned by [Escapement](escapement-core.md).
The previous SQL revision/frontier scheduler and storage contracts have been
removed. Acquirium compiles semantic input selections into complete native plans;
the engine owns affected-group execution, progress, historical repair, and atomic
source/derived publication.
