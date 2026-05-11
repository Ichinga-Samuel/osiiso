# GitHub Pages

The repository includes a MkDocs configuration and a GitHub Actions workflow for
publishing this documentation to GitHub Pages.

## Local preview

Install docs dependencies:

```bash
python -m pip install -e .[docs]
```

Serve locally:

```bash
mkdocs serve
```

Build strictly:

```bash
mkdocs build --strict
```

## GitHub setup

1. Push `mkdocs.yml`, `docs/`, and `.github/workflows/docs.yml`.
2. In the GitHub repository, open **Settings -> Pages**.
3. Set **Build and deployment** to **GitHub Actions**.
4. Push to `main`, `master`, or `dev`.

The workflow builds the site and deploys the generated artifact to Pages.
