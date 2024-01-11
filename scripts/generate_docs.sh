#!/bin/bash
set -e
echo "[START] Starting documentation generation"
if [ -d docs/autodoc ]; then
    echo "[*] Deleting autodoc directory..."
    rm -r docs/autodoc
fi
echo "[*] Generating testing documentation..."
pytest --cov-report="html:docs/autodoc/coverage" \
    --cov=src --html=docs/autodoc/test_report.md \
    --self-contained-html tests/test_code > /dev/null
echo "[*] Generating codestyle documentation ..."
pylint src | pylint-json2html -f jsonextended -o docs/autodoc/code_quality_report.md
# echo "[*] Changing html files to markdown..."
# find docs/autodoc/coverage/ -type f -name '*.html' -exec bash -c 'mv "$0" "${0%.html}.md"' {} \;
echo "[END] Finished generating documentation."
