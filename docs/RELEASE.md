# Release Process

This project uses Semantic Versioning and annotated tags for releases.

Steps to create a release:

1. Ensure `main` branch is up-to-date and all tests/CI pass.
2. Update `CHANGELOG.md` (if present) or the release notes.
3. Create an annotated tag:

```powershell
git checkout main
git pull
git tag -a vMAJOR.MINOR.PATCH -m "chore(release): vMAJOR.MINOR.PATCH"
git push origin --tags
```

4. Create a GitHub Release from the tag on the website or via `gh release create`.

Automated releases

- You can wire up a CI step to create releases automatically when a PR is merged and labeled `release`.
