#!/bin/bash

notebook_file="$1.ipynb"

jq -r '.cells[] | select(.cell_type=="code") | .source[]' "$notebook_file"
