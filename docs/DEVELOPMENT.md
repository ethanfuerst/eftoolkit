# Documentation

This directory contains the source files for the eftoolkit documentation site.

## Local Development

### Prerequisites

Install documentation dependencies:

```bash
uv sync
```

### Build and Serve

```bash
# Serve locally with hot reload
uv run mkdocs serve

# Open http://127.0.0.1:8000 in your browser
```

### Build Static Site

```bash
uv run mkdocs build

# Output is in site/ directory
```

## Deployment

Documentation is automatically deployed to GitHub Pages when:

1. Changes are pushed to `main` branch
2. A new tag/release is created

The deployment workflow is in `.github/workflows/docs.yml`.

### Manual Deployment

```bash
uv run mkdocs gh-deploy --force
```

## Adding New Pages

1. Create a new `.md` file in the appropriate directory:
   - `getting-started/` - Installation, quickstart
   - `user-guide/` - In-depth module documentation
   - `how-to/` - Recipes and patterns
   - `api/` - API reference (auto-generated)
   - `development/` - Contributing, changelog

2. Add the page to the `nav` section in `mkdocs.yml`:

   ```yaml
   nav:
     - User Guide:
       - New Page: user-guide/new-page.md
   ```

## Adding API Documentation

API docs are auto-generated from docstrings using mkdocstrings.

To document a new class or function, add it to the appropriate `docs/api/*.md` file:

```markdown
### MyClass

::: eftoolkit.module.MyClass
    options:
      show_root_heading: true
      show_source: true
```

## Versioning

For future versioned docs, we can use [mike](https://github.com/jimporter/mike):

```bash
# Deploy a version
uv run mike deploy 0.1.0 latest --update-aliases

# List versions
uv run mike list
```

Currently, we deploy only the latest version from `main`.

## File Structure

```
docs/
├── index.md                    # Home page
├── getting-started/
│   ├── installation.md
│   └── quickstart.md
├── user-guide/
│   ├── index.md
│   ├── duckdb.md
│   ├── s3.md
│   ├── gsheets.md
│   └── config.md
├── how-to/
│   └── index.md                # Recipes
├── api/
│   ├── index.md
│   ├── sql.md
│   ├── s3.md
│   ├── gsheets.md
│   └── config.md
├── development/
│   ├── contributing.md
│   └── changelog.md
└── README.md                   # This file
```
