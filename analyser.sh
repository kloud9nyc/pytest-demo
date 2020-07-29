#!/bin/bash
pytest --cov-report xml --cov=src/  tests/test_jobs/test_processSalesData.py -s

PATH_TO_PROJECT="$1"
SONAR_TOKEN="{YOUR-TOKEN}"
PROJECT_NAME="$( echo "$1" | sed 's@.*/@@' )"
cd src/
sonar-scanner \
  -Dsonar.projectKey=SalesTransactions \
  -Dsonar.sources=. \
  -Dsonar.host.url=http://localhost:9000 \
  -Dsonar.login=5474248ee4059424fcad3908166bb0428ce26427
