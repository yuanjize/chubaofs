#!/bin/bash
curl -v "http://127.0.0.1/admin/listVols?name=test" | python -m json.tool