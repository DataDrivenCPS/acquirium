---
title: Acquirium documentation
---

:::{div} version-warning
Acquirium is under rapid development. This documentation only explains version **{{ acquirium_version }}**.
:::

Acquirium is a data-metadata management platform for water treatment plants. It
pairs with [WaTr](https://github.com/DataDrivenCPS/water-ontology), an
ontology for water treatment operations that extends ASHRAE 223P with
treatment processes, substance flows, chemical media and references to
external data sources.

:::{div} sd-text-center sd-fs-2 sd-font-weight-bold sd-mt-5 sd-mb-3
New to Acquirium?
:::

```{button-ref} getting-started
:ref-type: doc
:color: primary
:shadow:
:align: center
:class: sd-fs-4 sd-px-5 sd-py-3 sd-mb-5

Get started
```

## Soft sensors

- [Build your first soft sensor](tutorials/advanced/first-app.md) by detecting high readings on a live signal.
- [Build a turbidity alarm pipeline](tutorials/turbidity.md) that cleans data and recalculates alarms when readings arrive late.
- [Write a soft sensor](how-to/write-soft-sensors.md) using your own streams, windows, and parameters.

## Get help and contribute

1. Found a bug or a performance issue? Open a [GitHub issue](https://github.com/DataDrivenCPS/acquirium/issues).
2. Feel free to fork the repo and open a [pull request](https://github.com/DataDrivenCPS/acquirium/pulls).
3. Join our community on [Slack](https://join.slack.com/t/acquirium/shared_invite/zt-4ek0gz21f-M~3mwGxSC1VgyPrzjyq9wQ)

## About this documentation

This documentation follows [Diátaxis](https://diataxis.fr):

- [Tutorials](tutorials/_index.md) are lessons. Start here if you are learning Acquirium.
- [How-to guides](how-to/_index.md) are steps for a specific task.
- [Reference](reference/_index.md) describes every method, config key and endpoint.
- [Explanation](explanation/_index.md) covers how Acquirium works and why.

## Citation

The design is described in [Acquirium: Toward Interoperable Data Driven
Applications in Water Treatment Systems](https://dl.acm.org/doi/abs/10.1145/3744256.3812557)
(BuildSys '26). If you use Acquirium in your work, cite:

```bibtex
@inproceedings{saka2026acquirium,
  title     = {Acquirium: Toward Interoperable Data Driven Applications in Water Treatment Systems},
  author    = {Saka, Umut Mete and Chapin, Fletcher T. and Paul, Lazlo and Anwar, Avia and Struck, Scott and Mauter, Meagan S. and Fierro, Gabe},
  booktitle = {Proceedings of the 13th ACM International Conference on Systems for Energy-Efficient Buildings, Cities, and Transportation (BuildSys '26)},
  year      = {2026},
  publisher = {ACM},
  address   = {Banff, AB, Canada},
  doi       = {10.1145/3744256.3812557}
}
```
