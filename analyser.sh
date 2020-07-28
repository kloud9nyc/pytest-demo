#!/bin/bash
PATH_TO_PROJECT="$1"
SONAR_TOKEN="{YOUR-TOKEN}"
PROJECT_NAME="$( echo "$1" | sed 's@.*/@@' )"
cd src/
sonar-scanner \
  -Dsonar.projectKey=SalesTransactions \
  -Dsonar.sources=. \
  -Dsonar.host.url=http://localhost:9000 \
  -Dsonar.login=43a67d849bc2b7798c4454261b437482b41d4902
