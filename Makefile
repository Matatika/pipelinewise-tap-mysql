venv:
	uv sync

lint:
	uv run ruff check tap_mysql/

unit_test:
	uv run pytest tests/unit --cov=tap_mysql --cov-report=html --cov-fail-under=47 $(extra_args)

integration_test:
	uv run pytest tests/integration --cov=tap_mysql --cov-report=html $(extra_args) -vvv

dbc:
	uvx dbc sync
