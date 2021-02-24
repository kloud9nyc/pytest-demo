#!/bin/bash
PATH_TO_PROJECT="$1"
SONAR_TOKEN="{YOUR-TOKEN}"
PROJECT_NAME="$( echo "$1" | sed 's@.*/@@' )"
cd src/
sonar-scanner \
  -Dsonar.projectKey=SalesTransactions1 \
  -Dsonar.sources=. \
  -Dsonar.host.url=http://172.20.10.2:9000 \
  -Dsonar.login=ed2cccfd4c6e9aaffd66d3fe49e7ba22f47ebe8a
