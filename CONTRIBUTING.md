# Contributing

Thank you for contributing! This document explains the contribution and versioning rules for this repository.

Branching and PRs

- `main` is the protected production branch.
- Create feature branches using `feature/<short-description>`.
- Always open a Pull Request (PR) to merge into `main`.
- Require at least one reviewer and that CI checks pass before merging.
- Use squash merges for small fixes and `Rebase and merge` for feature branches when history clarity is needed.

Commit messages

- Use conventional commits where possible:
  - `feat: ` for new features
  - `fix: ` for bug fixes
  - `chore: ` for maintenance
  - `docs: ` for documentation
  - `refactor: ` for refactors

Releases

- Follow Semantic Versioning (SemVer): `MAJOR.MINOR.PATCH`.
- Create annotated tags for each release, e.g. `git tag -a v1.2.0 -m "chore(release): v1.2.0"` and push the tag.

Security

- Do not commit credentials or secret files. Keep secrets in the GitHub repository secrets store.

CI / CD

- The repository includes a GitHub Actions template that deploys to Heroku. Configure secrets in the GitHub repo settings.
