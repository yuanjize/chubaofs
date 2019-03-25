#!/bin/bash
curl -v "http://127.0.0.1/metaPartition/update?id=1&isManual=false" | python -m json.tool