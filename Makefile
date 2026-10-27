.PHONY: all corpus cdc dbt catalog evaluate assets test api ui clean

all: corpus cdc dbt catalog evaluate

corpus:
	python -m src.synth_cdc_corpus
	python -m src.real_data

cdc:
	python -m src.cdc_loader

dbt:
	python -m src.warehouse --step all

catalog:
	python -m src.catalog_sync

evaluate:
	python -m src.evaluate

assets:
	python scripts/render_assets.py

test:
	pytest -q

api:
	uvicorn src.api:app --host 0.0.0.0 --port 8130

ui:
	streamlit run src/dashboard.py --server.port 8501

clean:
	rm -rf data/*.duckdb data/cdc_events data/ground_truth dbt/target dbt/logs
