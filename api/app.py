# app.py
# S2I entry point for OpenShift Python image
# This file tells S2I how to start the Django application

from riskplatform.wsgi import application