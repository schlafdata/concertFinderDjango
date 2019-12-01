from django.shortcuts import render

# Create your views here.
from django.shortcuts import render
from django.shortcuts import render, get_object_or_404
from django.http import Http404,HttpResponse,HttpResponseRedirect
import requests
from bs4 import BeautifulSoup
from api.scripts import concertfinderScript
import pandas as pd

from rest_framework.response import Response
from rest_framework.views import APIView
from .models import Matches
from .serializers import ArticleSerializer


def runScript(username):
    # try:
    import time
    start_time = time.time()

    result = concertfinderScript.findMatches(username)
    matches = result[0]
    # likes = result[1]
    # counts = result[1]
        # pd.set_option('max_colwidth', -1)
        # count = counts.to_html(classes=["table-striped", "table-hover",], index=False, justify="center", escape=False)
        # # pd.set_option('min_colwidth', 100)
        # data = matches.to_html(classes=["table-striped", "table-hover",], index=False, justify="center", escape=False)
        # time = 'Runtime: ' + str(round((time.time() - start_time),2)) + ' seconds'

    # except:
    #     data = 'dam daniel'
        # error = 'Please enter a valid Soundcloud username!'
        # errorDf = pd.DataFrame([error])
        # errorDf.columns = ['An Error Occured']
        # data = errorDf.to_html(justify='center', index=False)
        # time = 'Script never completed :('
        # count = 'damn daniel'


        # args = {'form': form, 'text':data, 'time':time, 'count':count}
    return matches

class ArticleView(APIView):
    def get(self, request):
        # articles = Article.objects.all()

        user = str(request.GET['username']).rstrip('/')
        data = runScript(user)
        # the many param informs the serializer that it will be serializing more than a single article.
        serializer = ArticleSerializer(data, many=True)
        resp = {'Matches' : serializer.data}
        return Response(resp)
