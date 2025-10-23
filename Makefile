PYTHON=python3
VENV=venv_vendas

.PHONY: test run dashboard migrate lint aggregate

venv:
	python3 -m venv $(VENV)
	$(VENV)/bin/python -m pip install --upgrade pip
	$(VENV)/bin/python -m pip install -r requirements.txt

test:
	$(PYTHON) -m pytest -q

run:
	$(PYTHON) main.py run

dry-run:
	$(PYTHON) main.py dry-run

generate-sample:
	$(PYTHON) main.py generate-sample --sample-size 100

dashboard:
	streamlit run dashboard/app.py --server.headless true

migrate:
	$(PYTHON) main.py migrate

aggregate:
	$(PYTHON) scripts/aggregate_duplicates.py
