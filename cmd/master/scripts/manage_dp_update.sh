#!/bin/bash
curl -v "http://127.0.0.1/dataPartition/update?id=4&isManual=true"  | python -m json.tool