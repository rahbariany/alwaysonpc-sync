# AlwaysOnPC Sync (suggested repo name: `alwaysonpc-sync`)

This repository contains the AlwaysOnPC automation and sync utilities.
It includes Credinvest SFTP sync, Vestr fee fetching, Dropbox uploads and PostgreSQL aggregation utilities.

Quick start

1. Create a GitHub repository (see instructions below) and push this folder.
2. Create a Python virtual environment and install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

3. Run the main executable locally (examples):

```powershell
# Run the integrated executable (built with PyInstaller)
.\dist\integrated_sync_enhanced.exe --help

# Or run the Python entrypoint
python integrated_sync_enhanced.py --skip-credinvest --verbose
```

Repository conventions

- Branches: `main` is protected (PRs required). Feature branches use `feature/<short-desc>`.
- Pull Requests: Use the template in `.github/PULL_REQUEST_TEMPLATE.md`. Require at least one reviewer and CI success before merge.
- Releases: Use semantic versioning (`vMAJOR.MINOR.PATCH`) and create annotated tags for releases.

Deployments

This project can be deployed to Heroku (similar to other projects in the workspace). A GitHub Actions workflow is included as a template; set the following repository secrets on GitHub before enabling automatic deploys:

- `HEROKU_API_KEY`
- `HEROKU_APP_NAME`

Creating the remote GitHub repository and pushing

```powershell
# From the AlwaysOnPC folder
git init
git add .
git commit -m "chore: initial repo scaffold"
# Create a GitHub repo via CLI (gh) or on the website
gh repo create rahbariany/alwaysonpc-sync --public --source=. --remote=origin --push
# OR
# 1) Create repo on github.com
# 2) Add remote and push:
# git remote add origin https://github.com/<your-org-or-user>/<repo>.git
# git branch -M main
# git push -u origin main
```

If you want me to create the remote repo and push, I can prepare the commands and help guide you through the `gh` CLI flow.

Contact & Notes

- This scaffold was generated to match the workspace conventions used by your other projects (Bitbucket/Heroku deploy pipeline translated to a GitHub Actions template).
- Suggested repo name: `alwaysonpc-sync`. Rename as needed.
