# Repository Guidelines

## Project Structure & Module Organization
This repository combines a shared Python crawler core with an Android wrapper.

- `spider_core/` contains the FastAPI app, crawler engine, SQLite storage, and shared runtime code.
- `spider_core/frontend/` stores the WebUI static assets (`index.html`, logo, favicon).
- `spider_core/tests/` contains Python `unittest` coverage.
- `app/src/main/java/com/javdb_spider/app/` contains the Android Java entry points, WebView bridge, and service code.
- `app/src/main/res/` contains Android layouts, values, icons, and XML resources.
- Root files such as `Dockerfile`, `requirements.txt`, and Gradle files define local, Docker, and Android builds.

## Agent Behavior & Change Discipline
Think before coding. State assumptions before implementing, surface tradeoffs when there are multiple valid interpretations, and ask for clarification instead of guessing when requirements are unclear. If a simpler approach would satisfy the request, prefer it and explain the tradeoff.

Keep solutions minimal and goal-driven. Do not add features, abstractions, configurability, or defensive handling that was not requested. For multi-step work, define brief success criteria and verification steps before changing code, such as reproducing a bug with a focused test before fixing it.

Make surgical changes. Touch only files and lines that directly support the user request, match the existing style even when another style is preferable, and avoid refactoring adjacent code unless it is necessary for the task. Clean up imports, variables, functions, or generated artifacts that your own change made unused, but do not remove pre-existing dead code unless explicitly asked.

Verify changes with the lightest reliable check for the risk involved. Prefer targeted tests for database schema, task state transitions, security validation, URL handling, and export behavior. If verification cannot be run, state the reason and describe the static checks performed.

## Build, Test, and Development Commands
- `pip install -r requirements.txt`: install Python dependencies.
- `cd spider_core && python -m uvicorn main:app --host 0.0.0.0 --port 8000`: run the WebUI and API locally.
- `python -m unittest discover -s spider_core/tests`: run the Python test suite.
- `./gradlew test`: run Android unit tests.
- `./gradlew assembleDebug`: build a debug APK with the Chaquopy-packaged Python core.
- `docker build -t javdb-spider .`: build the container image.

## Coding Style & Naming Conventions
Use Python 3.12-compatible code. Follow existing Python style: 4-space indentation, snake_case functions and variables, and `unittest.TestCase` classes named by behavior or feature. Java code should follow Android conventions: package `com.javdb_spider.app`, PascalCase classes, camelCase methods, and resources named in lowercase snake_case.

Prefer structured storage helpers in `db_store.py` and path utilities in `storage_utils.py` instead of duplicating database or filesystem logic.

## Testing Guidelines
Python tests live in `spider_core/tests/` and use files named `test_*.py`. Keep storage tests isolated with temporary directories, matching the existing `TemporaryDirectory` pattern. Add or update tests when changing database schema, task state transitions, security validation, URL handling, or export behavior.

Run `python -m unittest discover -s spider_core/tests` before submitting Python changes. Run `./gradlew test` when Android Java or Gradle configuration changes.

## Commit & Pull Request Guidelines
Recent commits use short imperative summaries such as `Add tag parsing and filtered exports`, with occasional prefixes like `docs:`. Keep the first line concise.

Pull requests should describe the change, list test commands run, mention affected targets (`PC`, `Docker`, `Android`), and include screenshots for visible WebUI or Android UI changes.

## Security & Configuration Tips
Do not commit real cookies, tokens, databases, generated CSV exports, or signing credentials. Use `JAVDB_AUTH_TOKEN` for PC/Docker API protection, and preserve the existing path validation safeguards when adding file operations.

## Dependency Compatibility (Pydantic v1/v2)
The crawler core ships against two dependency sets and must run on both:
- **PC/Docker** — `requirements.txt`: FastAPI ≥0.136, **Pydantic v2**.
- **Android (Chaquopy)** — `app/build.gradle`: FastAPI 0.95, **Pydantic v1** (`>=1.10.24,<2.0.0`). Pydantic v2's `pydantic-core` is a Rust extension with no Chaquopy-compatible wheel, so v1 is intentional, not a mistake.

Therefore `schemas.py` and every router/service must stay within the **v1/v2 common subset**:
- Do NOT use v2-only APIs (`model_dump`, `model_validate`, `model_config`, `ConfigDict`, `@field_validator`) or v1-only APIs removed in v2 (`parse_obj`, `parse_raw`).
- Use plain `BaseModel` field declarations and `X | None` for optionals (Python 3.12 supports PEP 604 on both versions).

CI enforces this with a grep guard in the test job. CI runs the suite on Pydantic v2 (`requirements.txt`); the local test venv at `spider_core/.venv-test` mirrors the Android set (Pydantic v1) — running both covers both versions.
