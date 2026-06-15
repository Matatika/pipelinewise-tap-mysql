venv:
	uv venv --clear ;\
	. ./.venv/bin/activate ;\
	uv pip install -e .[test]

lint:
	. ./.venv/bin/activate ;\
	ruff check tap_mysql/

unit_test:
	. ./.venv/bin/activate ;\
	pytest tests/unit --cov=tap_mysql --cov-report=html --cov-fail-under=47 $(extra_args)

integration_test:
	. ./.venv/bin/activate ;\
	pytest tests/integration --cov=tap_mysql --cov-report=html $(extra_args) -vvv
