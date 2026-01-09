.PHONY: install run api dashboard clean

install:
	pip install -r requirements.txt

api:
	uvicorn api.main:app --reload

run:
	python pipeline.py

dashboard:
	streamlit run analytics_app.py

clean:
	rm -rf data_lake/landing/* data_lake/temp/*

stats:
	python scripts/lake_stats.py
