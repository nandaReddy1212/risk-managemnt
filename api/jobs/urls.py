from rest_framework.routers import DefaultRouter
from django.urls import path, include
from .views import SparkJobViewSet

router = DefaultRouter()
router.register(r'', SparkJobViewSet, basename='sparkjob')

urlpatterns = [
    path('', include(router.urls)),
]