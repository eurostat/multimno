#!/bin/bash
echo "[START] Starting documentation generation"
echo "[*] Generating testing documentation..."
pytest --cov-report="html:docs/tests/coverage" \
    --cov=src --html=docs/tests/test_report.html \
    --self-contained-html tests/test_code > /dev/null
echo "[*] Generating codestyle documentation ..."
pylint src | pylint-json2html -f jsonextended -o docs/code_quality_report.html