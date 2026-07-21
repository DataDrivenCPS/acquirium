# Publishing Acquirium — Cheatsheet

## Semantic Versioning (SemVer) in one screen

A version is `MAJOR.MINOR.PATCH`, e.g. `1.4.2`.

| Bump  | When                                                    | Backward compatible? |
|-------|---------------------------------------------------------|----------------------|
| MAJOR | You break the public API (rename/remove/change behavior) | No                   |
| MINOR | You add new functionality                                | Yes                  |
| PATCH | You only fix bugs                                        | Yes                  |

**`0.y.z` — initial development.** While `MAJOR == 0`, the public API is **not** stable. Breaking changes are allowed in any release; bump `MINOR` for them, `PATCH` for bug fixes. Acquirium is currently in this phase.

**`1.0.0` — public commitment.** Cut this only when you're ready to promise: no breaking changes to the public Python API without a `MAJOR` bump.

**Pre-releases.** Append `-rc.1`, `-beta.1`, `-alpha.1` (e.g. `1.0.0-rc.1`) for release candidates. Lower precedence than the un-suffixed release.

**Rules to remember**
- Versions are immutable on PyPI/TestPyPI. Once `0.1.1` is uploaded, it cannot be replaced — only superseded by `0.1.2`.
- Tags must match `pyproject.toml`. The release workflow fails fast if they diverge.
- Use `CHANGELOG.md` to track what changed under each version.

## Release commands

### 0. Make sure the working tree is clean

```bash
git status
git pull --ff-only
```

### 1. Bump the version

```bash
uv version --bump patch    # bug fixes only       (0.1.1 -> 0.1.2)
uv version --bump minor    # new features / breaking changes while 0.y.z  (0.1.x -> 0.2.0)
uv version --bump major    # only when cutting 1.0.0                       (0.x   -> 1.0.0)
```

Edit `CHANGELOG.md`: move entries from `[Unreleased]` under a new `## [x.y.z] - YYYY-MM-DD` heading and update the link references at the bottom.

### 2. Commit and tag

```bash
git add pyproject.toml uv.lock CHANGELOG.md
git commit -m "Release vX.Y.Z"
git tag vX.Y.Z
```

### 3. Publish to TestPyPI (tag push)

```bash
git push origin <branch> vX.Y.Z
```

Pushing a `v*` tag triggers `.github/workflows/release.yml` →
`build` → `publish-testpypi` (gated on the `testpypi` GitHub environment).

Watch:

```bash
gh run watch
```

Then verify the install in a throwaway venv:

```bash
python3.12 -m venv /tmp/aq && source /tmp/aq/bin/activate
uv pip install --no-cache \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/ \
  acquirium==X.Y.Z
acquirium server   # smoke test
```

The `--extra-index-url` is required: Acquirium itself is on TestPyPI, but its
dependencies live on real PyPI.

### 4. Publish to PyPI (GitHub Release)

```bash
gh release create vX.Y.Z --generate-notes --title "vX.Y.Z"
```

Creating a GitHub Release fires the `release: published` event and runs
`publish-pypi` (gated on the `pypi` GitHub environment).

Verify:

```bash
uv pip install --no-cache acquirium==X.Y.Z
```

Project page: <https://pypi.org/project/acquirium/>.

## Hotfix something that broke a release

You can't replace a published version. Instead:

```bash
# fix the bug, then:
uv version --bump patch
# update CHANGELOG.md
git commit -am "Release vX.Y.(Z+1) — fix <thing>"
git tag vX.Y.(Z+1)
git push origin <branch> vX.Y.(Z+1)        # -> TestPyPI
gh release create vX.Y.(Z+1) --generate-notes    # -> PyPI
```

## One-time setup (already done)

These are wired up and don't need to be redone for normal releases:

- **PyPI Trusted Publisher** for `acquirium` ↔ `DataDrivenCPS/acquirium` ↔ workflow `release.yml` ↔ environment `pypi`.
- **TestPyPI Trusted Publisher** with the same mapping but environment `testpypi`.
- GitHub repository environments `pypi` and `testpypi` created in **Settings → Environments**.

If you ever transfer the repo, rename it, or rename the workflow file, you must update both Trusted Publisher records.
