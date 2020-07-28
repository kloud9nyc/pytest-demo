build-dev:
	rm -rf dist-dev
	mkdir ./dist-dev
	cp ./src/jobs/processSalesData.py ./dist-dev
	zip -x /Users/raghunathan.bakkianathan/Work/DataEngg/venv/lib/python3.7/site-packages/ -r dist-dev/jobs.zip .


build-prod:
	rm -rf dist-prod
	mkdir ./dist-prod
	cp ./src/jobs/processSalesData.py ./dist-prod
	zip -x /Users/raghunathan.bakkianathan/Work/DataEngg/venv/lib/python3.7/site-packages/ -r dist-prod/jobs.zip .
