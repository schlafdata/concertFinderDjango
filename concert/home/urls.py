from django.conf.urls import url
from home.views import HomeView
from django.urls import path

urlpatterns = [
    url(r'^$', HomeView.as_view(template_name="home/main.html")),
]
