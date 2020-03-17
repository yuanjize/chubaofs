#!/bin/bash
curl -v "http://127.0.0.1/metaPartition/get?id=1" | python -m json.tool