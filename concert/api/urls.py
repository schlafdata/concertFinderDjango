
from django.urls import path

from .views import ArticleView


app_name = "api"

# app_name will help us do a reverse look-up latter.
urlpatterns = [
    path('api/', ArticleView.as_view()),
]
