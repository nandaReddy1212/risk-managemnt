#!/bin/bash
exec gunicorn riskplatform.wsgi --bind 0.0.0.0:8080 --workers 2