#!/bin/bash

# Copy the pre-commit hook
cp resources/hooks/pre-commit .git/hooks/pre-commit

# Make the hook script executable
chmod +x .git/hooks/pre-commit