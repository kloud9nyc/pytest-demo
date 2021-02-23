#!/bin/bash
PATH_TO_PROJECT="$1"
SONAR_TOKEN="{YOUR-TOKEN}"
PROJECT_NAME="$( echo "$1" | sed 's@.*/@@' )"
cd src/
sonar-scanner \
  -Dsonar.projectKey=SalesTransactions \
  -Dsonar.sources=. \
  -Dsonar.host.url=http://localhost:9000 \
  -Dsonar.login=c203d789a28b4f201ebbe09ccfa8d6b0f7fabde9
