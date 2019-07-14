from django.views.generic import TemplateView
from home.forms import HomeForm
from django.shortcuts import render
import requests
from bs4 import BeautifulSoup
from home.scripts import concertfinder_test2
import pandas as pd

class HomeView(TemplateView):
    template_name = 'home/main.html'

    def get(self, request):
        form = HomeForm()
        return render(request, self.template_name, {'form': form})

    def post(self, request):
        form = HomeForm(request.POST)
        if form.is_valid():
            text = form.cleaned_data['post']

        try:
            import time
            start_time = time.time()
            result = concertfinder_test2.findMatches(text)
            matches = result[0]
            counts = result[1]
            pd.set_option('max_colwidth', -1)
            count = counts.to_html(classes=["table-striped", "table-hover",], index=False, justify="center", escape=False)
            # pd.set_option('min_colwidth', 100)
            data = matches.to_html(classes=["table-striped", "table-hover",], index=False, justify="center", escape=False)
            time = 'Runtime: ' + str(round((time.time() - start_time),2)) + ' seconds'

        except:
            error = 'Please enter a valid Soundcloud username!'
            errorDf = pd.DataFrame([error])
            errorDf.columns = ['An Error Occured']
            data = errorDf.to_html(justify='center', index=False)
            time = 'Script never completed :('
            count = 'damn daniel'


        args = {'form': form, 'text':data, 'time':time, 'count':count}
        return render(request, self.template_name, args)
