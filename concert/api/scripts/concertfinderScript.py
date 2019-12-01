import pycrfsuite
# import pycrfsuite
import pandas as pd
import requests
# import numpy as np
from datetime import datetime
from dateutil.parser import parse
from bs4 import BeautifulSoup
import json
import itertools
from urllib.parse import urljoin
import warnings
warnings.filterwarnings("ignore")
import nltk
import re
from concurrent import futures
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import os.path, sys
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

tagger = pycrfsuite.Tagger()

# crf = os.path.join(sys.path[9], '/home/schlafdata/concertFinderDjango/concert/home/scripts/crf.model')
tagger.open('/Users/jschlafly/concert_finder/concertFinderDjango/concert/home/scripts/crf.model')

cred = credentials.Certificate('/Users/jschlafly/concert_finder/creds.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

proxies = {}

def userData(user):
    # Use a service account
    # cred = credentials.Certificate('/Users/jschlafly/concert_finder/creds.json')
    # firebase_admin.initialize_app(cred)
    # db = firestore.client()


    if user not in [doc.id for doc in db.collection(u'users').stream()]:
        #add that user to the db with filler data
        db.collection(u'users').document(user).set({'maxTimeStamp':'1950-11-24T00:00:00Z','likedArtists':[]})

    # if user had data in database then.. bring that data in
    doc_ref = db.collection(u'users').document(user)
    doc = doc_ref.get()

    max_timestamp = doc.to_dict()['maxTimeStamp']
    liked_artists = doc.to_dict()['likedArtists']

    return max_timestamp, liked_artists

def userLikes(user):

    headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Referer': 'https://soundcloud.com/',
    'Origin': 'https://soundcloud.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
    'Authorization': 'OAuth 2-290287-109566302-rP3anBZUUFsMKf',
    }

    params = (
        ('limit', '1000'),
        ('app_version', '1557747132'),
        ('app_locale', 'en'),
    )

    userHistory = userData(user)

    if userHistory is not None:
        maxTime = userHistory[0]
        artists = userHistory[1]


    userInfo = requests.get('https://soundcloud.com/{}/'.format(user), proxies=proxies, verify=False)
    userSoup = BeautifulSoup(userInfo.text, 'html.parser')

    userId = str(userSoup).split('https://api.soundcloud.com/users/')[1].split('"')[0]


    nextHref = 'https://api-v2.soundcloud.com/users/{}/likes'.format(userId)
    tracks = []
    # playlists = []

    dateCheck = 0

    while (nextHref != None ) and (dateCheck == 0):

        response = requests.get(nextHref, headers=headers, params=params, proxies=proxies, verify=False)
        jsonLikes = json.loads(response.text)

        userTracks = [(x['track'], x['created_at']) for x in jsonLikes['collection'] if 'track' in list(x.keys())]
        trackLabels = ['title','user','label_name','publisher_metadata','artwork_url','genre','id','permalink_url','likes_count','user_id']


        trackData = []
        for x in userTracks:
            trackInfo = [x[0].get(label) for label in trackLabels]
            if pd.to_datetime(maxTime) > pd.to_datetime(x[1]):
                dateCheck = 1
            trackInfo.append(x[1])
            trackData.append(trackInfo)

        tracks.append(trackData)

        nextHref = jsonLikes['next_href']

    data = pd.concat(pd.DataFrame(tracks[x]) for x in range(0, len(tracks)))

    data.columns = ['title','user','label_name','publisher_metadata','artwork_url','genre','id','permalink_url','likes_count','user_id','created_at']

    data['username'] = data.user.map(lambda x : x.get('username'))
    data['publisher'] = data.publisher_metadata.map(lambda x : x if x is None else x.get('publisher'))
    data['artist'] = data.publisher_metadata.map(lambda x : x if x is None else x.get('artist'))
    data['writer_composer'] = data.publisher_metadata.map(lambda x : x if x is None else x.get('writer_composer'))
    data['publisher_id'] = data.publisher_metadata.map(lambda x : x if x is None else x.get('id'))

    data = data[['title', 'created_at', 'label_name',
           'artwork_url', 'genre', 'id', 'permalink_url', 'likes_count',
           'username', 'publisher', 'artist', 'writer_composer', 'publisher_id','user_id']]

    data = data[['title','username','artist','publisher','writer_composer','label_name','created_at','likes_count','artwork_url','permalink_url','id','genre','publisher_id','user_id']]

    return data, artists

def removeEmoji(row):
    return row.encode('ascii', 'ignore').decode('ascii').strip()

def remixArtist(row):

    remixAlias = ['REREMIX','RMX','VIP', 'FLIP','EDIT','REMIX']

    contents = re.findall('\(.*?\)|\[.*?\]',row.title)
    artists = []
    for content in contents:
        for alias in remixAlias:
            if alias in content.upper():
                artists.append(content[:content.upper().find(alias)].strip('(|[').strip())
                break

    artists = [x for x in artists if (x != '') & (x is not None)]
    if len(artists) == 1:
        return artists[0]
    elif len(artists) > 1:
        for artist in artists:
            if artist.upper() == row.username.upper():
                return artist
            elif artist.upper() == row.username.upper():
                return artist
            else:
                return
    else:
        return


def usernameMatch(row):

    if (row.foundArtist is None):
        if row.title.count('-')==1:
            try:
                titleSplit = [x.upper().strip() for x in row.title.split('-')]
                for x in titleSplit:
                    if x == str(row.username).upper().strip():
                        return row.username
                    elif x == str(row.artist).upper().strip():
                        return row.artist
            except:
                return
    else:
        return row.foundArtist


def featuring(row):

    if row.foundArtist is None:
        featSplits = [x.strip() for x in re.split('FT.|FEAT.|FEATURING|-|FEAT|\(.*?\)|\[.*?\]', row.title.upper()) if x is not None]
        if (len(featSplits)>1) & (row.title.count('-')==1):
            for split in featSplits:
                try:
                    if (row.username.upper() in split.upper()):
                        return split
                    elif (row.artist is not None) & (row.artist.upper() in split.upper()):
                        return split
                except:
                    return
    else:
        return row.foundArtist


def userArtistMatch(row):
    if row.foundArtist is None:
        if row.title.count('-')==0:
            try:
                if row.username.upper() == row.artist.upper():
                    return row.username
            except:
                return
        else:
            return
    else:
        return row.foundArtist



def collabs(row):
    if row.foundArtist is None:
        if row.title.count('-') == 1:
            leftEl = row.title.split('-')[0]
            try:
                elList = [x.strip() for x in re.split(' & | X | AND |,', leftEl.upper())]
                artistList = [x.strip() for x in re.split(' & | X | AND |,', row.artist.upper())]
                if elList == artistList:
                    return leftEl
            except:
                return
    else:
        return row.foundArtist


def usernameDash(row):
    if row.foundArtist is None:
        if row.username.count('-') > 0:
            try:
                leftEl = "-".join(row.title.split("-", 2)[:2])
                if row.username.upper() in leftEl.upper():
                    return leftEl
            except:
                return
        else:
            return
    else:
        return row.foundArtist

def noDash(row):
    if row.foundArtist is None:
        if row.title.count('-')==0:
            return row.username


def maybe(row):
    if (row.foundArtist is None) & (row.maybeArtist is None):
        if row.title.count('-') >0:
            return row.title.split('-')[0]
    else:
        return row.maybeArtist


def userArtist(row):
    if (row.foundArtist) and (row.foundArtist.upper()==row.username.upper()):
        return [(row.username, row.user_id, row.id, True)]

def multArtists(row):
    if (row.foundArtist) and (row.allArtist is None):
        splitArtists = [x.strip() for x in re.split(' & | X |,| x | and | AND ', row.foundArtist) if (x != ' ') & (x is not None) & (x != '')]
        artistList = []
        for artist in splitArtists:
            if artist.upper()==row.username.upper():
                artistData = (artist, row.user_id, row.id, True)
                artistList.append(artistData)
            else:
                artistData = artist, [], row.id, True
                artistList.append(artistData)
        if artistList:
            return artistList
        else:
            return
    else:
        return row.allArtist

def maybeArtists(row):
    if (row.foundArtist is None) and (row.allArtist is None):
        splitArtists = [x.strip() for x in re.split(' & | X |,| x ', row.maybeArtist) if (x != ' ') & (x is not None) & (x != '')]
        artistList = []
        for artist in splitArtists:
            artistData = (artist, [], row.id, False)
            artistList.append(artistData)
        return artistList
    else:
        return row.allArtist



def mapFilters(user):


    userData = userLikes(user)
    userHistory = userData[1]

    data = userData[0]
    data['title'] = data.title.map(removeEmoji)
    data['username'] = data.username.map(removeEmoji)
    data['foundArtist'] = data.apply(remixArtist,axis=1)
    data['foundArtist'] = data.apply(usernameMatch, axis=1)
    data['foundArtist'] = data.apply(featuring, axis=1)
    data['foundArtist'] = data.apply(userArtistMatch, axis=1)
    data['foundArtist'] = data.apply(collabs, axis=1)
    data['foundArtist'] = data.apply(usernameDash, axis=1)
    data['maybeArtist'] = data.apply(noDash, axis=1)
    data['maybeArtist'] = data.apply(maybe, axis=1)
    data['allArtist'] = data.apply(userArtist, axis=1)
    data['allArtist'] = data.apply(multArtists, axis=1)
    data['allArtist'] = data.apply(maybeArtists, axis=1)

    newLikes = list(set([artist[0] for row in data['allArtist'] for artist in row]) - set(userHistory))
    maxTimeStamp = data['created_at'].max()


    userDB = db.collection(u'users').document(user)

    if newLikes != []:
        userDB.update({u'maxTimeStamp': maxTimeStamp})
        userDB.update({u'likedArtists': firestore.ArrayUnion(newLikes)})
    else:
        pass

    oldLikes = userHistory
    userArtistLikes = list(set(newLikes + oldLikes))


    return [userArtistLikes]


# In[126]:


proxies = {
}

def temple_scrape():

    temple_events = requests.get('https://www.templedenver.com/event-calendar/', proxies = proxies, verify=False)
    temple_soup = BeautifulSoup(temple_events.text, "html.parser")
    event_table = temple_soup.findAll('div', {'class':'uv-calendar-list uv-integration uv-eventlist uv-eventlist-default uv-clearfix'})

    artist_list = []
    dates_list = []

    for event,link in zip(event_table[0].findAll('div', {'class':'uv-name'}), event_table[0].findAll('a',{'class':'uv-btn uv-btn-s uv-btn-100'})):
        if re.search('PRE-PARTY', event.contents[0]):
            req = requests.get(link['href'], proxies = proxies, verify=False)
            respSoup = BeautifulSoup(req.text, 'html.parser')
            artist_list.append(respSoup.find('div', {'class':'uv-evdescr'}).contents[1].text.split('featuring')[1].split('.')[0])
        else:
            artist_list.append(event.contents[0])


    for x in event_table[0].findAll('div', {'class':'uv-date-list uv-oneline uv-showdesk'}):
        dates_list.append(x.contents[0])

    link = []
    for x in temple_soup.findAll('a', {'class':'uv-btn uv-btn-s uv-btn-100'}):
        link.append(x['href'])


    temple_events = pd.DataFrame(list(zip(artist_list,dates_list,link)))
    temple_events.columns = ['Artist','Date','Link']
    temple_events['Venue'] = 'Temple'

    def splitArtists(row):
        return [x.strip() for x in re.split('w/|B2B|,', row) if x != ' ']
    temple_events['FiltArtist'] = temple_events['Artist'].map(splitArtists)

    return temple_events

def mishScrape():
    mishQuery = requests.get('https://mishawaka.ticketforce.com/include/widgets/events/EventList.asp?category=&page=1', proxies = proxies, verify=False)
    mishJson = mishQuery.json()

    mishData = []
    for event in mishJson['swEvent']:
        headline = event['Event']
        desc = event['Description']
        date = event['PerformanceStart']
        id_ = event['upcomingInfo']['EventID']
        link = 'https://mishawaka.ticketforce.com/eventperformances.asp?evt={0}'.format(id_)
        pic = 'https://mishawaka.ticketforce.com/uplimage/' + event['Img_1']

        descSoup = BeautifulSoup(desc, 'html.parser')
        try:
            info = descSoup.find('span', {'style':'font-size: 18pt; color: #ff6600;'}).text
        except:
            try:
                info = descSoup.find('span', {'style':'color: #ff6600;'}).text
            except:
                info = headline

        res = (info, date,link, 'Mishawaka Amphitheatre', pic)
        mishData.append(res)

    mishFrame = pd.DataFrame(mishData)
    mishFrame.columns = ['Artist','Date','Link','Venue','picLink']
    mishFrame['Date'] = mishFrame['Date'].map(lambda x : x.split('T')[0])
    mishFrame['Date'] = pd.to_datetime(mishFrame['Date'])
    mishFrame['Date'] = mishFrame.Date + pd.Timedelta(days=-1)
    mishFrame['Date'] = mishFrame['Date'].map(lambda x : str(x))

    def splitArtists(row):
        return [x.strip() for x in re.split('w/|with|,|special guests', row) if x != ' ']
    mishFrame['FiltArtist'] = mishFrame['Artist'].map(splitArtists)

    return mishFrame

def fillmore_scrape():

    fillmore = requests.get('http://www.fillmoreauditorium.org/events/', proxies = proxies, verify=False)
    fillmore_soup = BeautifulSoup(fillmore.text, 'html.parser')
    fillmore_json = fillmore_soup.findAll('script',{'type':'application/ld+json'})
    fillmore_string = str(fillmore_json).split('</script>, <script type="application/ld+json">')
    fillmore_string[0] = fillmore_string[0].split('[<script type="application/ld+json">\n')[1]
    fillmore_string[-1] = fillmore_string[-1].split('</script>]')[0]

    filmore_events = []

    for x in fillmore_string[1:]:
        y = json.loads(x)
        data = (y['name'],y['startDate'].split('T')[0],y['offers']['url'],y['image'], 'Fillmore Auditorium')
        filmore_events.append(data)

    filmore_events_frame = pd.DataFrame(filmore_events)
    filmore_events_frame.columns = ['Artist','Date','Link','picLink','Venue']

    def splitArtists(row):
        return [x.strip() for x in re.split(';|:|,', row) if x != ' ']
    filmore_events_frame['FiltArtist'] = filmore_events_frame['Artist'].map(splitArtists)

    return filmore_events_frame

def redRocks():
    redRocks = requests.get('https://www.redrocksonline.com/events/category/Concerts', proxies = proxies, verify=False)
    redSoup = BeautifulSoup(redRocks.text, 'html.parser')
    redInfo = redSoup.findAll('div', {'class':'m-info-container'})
    thumb = redSoup.findAll('div', {'class':'thumb'})

    details = []
    for x in redInfo:
        try:
            artists = x.findAll('h3')[0].contents[0] +',' +  x.findAll('h4')[0].contents[0]
        except:
            artists = x.findAll('h3')[0].contents[0]
        for y in x.findAll('div', {'class':'m-date m-details'}):
            try:
                date = (y.findAll('span', {'class':"m-date__month"})[0].contents[0] + y.findAll('span', {'class':"m-date__day"})[0].contents[0] + y.findAll('span', {'class':"m-date__year"})[0].contents[0])
            except:
                pass
        try:
            link = x.find('a', {'class','tickets btn btn-large'})['href']
        except:
            pass

        info_ = (artists, date,link)
        details.append(info_)

    pic = []
    for x in thumb:
        for img in x.find_all('img', alt=True):
            pic.append(img['src'])
    redRocksFrame = pd.DataFrame(details)
    redRocksFrame.columns = ['Artist','Date','Link']
    redRocksFrame['Venue'] = 'Red Rocks'
    redRocksFrame['picLink'] = pic

    def splitArtists(row):
        return [x.strip() for x in re.split(';|:|,|,with|/|special guest|with|-', row) if x != '']

    redRocksFrame['FiltArtist'] = redRocksFrame['Artist'].map(splitArtists)

    return redRocksFrame

def black_box_scrape():

    black_box = requests.get('https://blackboxdenver.ticketfly.com/', proxies = proxies, verify=False)
    black_soup = BeautifulSoup(black_box.text, 'html.parser')
    black_event = black_soup.select('article[class*="list-view-item"]')

    black_box_events = []
    links = []
    for x in black_event:
        headliners = ', '.join([x.text for x in x.select('h1[class*=headliners]')])
        try:
            support = x.find('h2', {'class':'supports'}).text
            artists = headliners + ' ' + support
            event_data = (artists, x.find('span', {'class':'dates'}).text, 'Black Box')
            black_box_events.append(event_data)
        except:
            artists = headliners
            event_data = (artists, x.find('span', {'class':'dates'}).text, 'Black Box')
            black_box_events.append(event_data)

        try:
            link = 'https://www.ticketfly.com/purchase' + x.find('a', href=True)['href']
            links.append(link)
        except:
            link = 'Tickets at the door'
            links.append(link)

    pic = []
    for img in black_soup.find_all('img', alt=True):
        pic.append(img['src'])

    black_box_events = pd.DataFrame(black_box_events)
    black_box_events.columns = ['Artist','Date','Venue']
    black_box_link = pd.DataFrame(links)
    black_box_events = pd.concat([black_box_events,black_box_link], axis=1)
    black_box_events.columns = ['Artist','Date','Venue','Link']
    black_box_events = black_box_events[['Artist','Date','Link','Venue']]

    black_box_events['picLink'] = pic[1:]

    def splitArtists(row):
        return [x.strip() for x in re.split('&|(Master Class)|presents|,|:|;|\sand\s|ft.|b2b', row) if (x is not None) & (x != '')]

    black_box_events['FiltArtist'] = black_box_events['Artist'].map(splitArtists)
    black_box_events['Date'] = black_box_events['Date'].map(lambda x : x.replace('.','/'))

    return black_box_events


def blue_bird_scrape():

    blue_bird = requests.get('https://www.bluebirdtheater.net/events', proxies = proxies, verify=False)
    bird_soup = BeautifulSoup(blue_bird.text, 'html.parser')
    blue_bird_events = list(zip(list(filter(None, [z.strip() for z in [y[0] for y in [x.contents for x in bird_soup.findAll('a', {'title':"More Info"})]]])),[x.contents[2].strip() for x in bird_soup.findAll('span', {'class':"date"})],[x['href'] for x in bird_soup.findAll('a',{'class':'btn-tickets accentBackground widgetBorderColor secondaryColor tickets status_1'})]))
    blue_bird_events = pd.DataFrame(blue_bird_events)
    blue_bird_events['Venue'] = 'Blue Bird'
    blue_bird_events.columns = ['Artist','Date','Link','Venue']

    pics = []
    thumbs = bird_soup.findAll('div', {'class':'thumb'})
    for x in thumbs:
        pics.append(x.find('img', alt=True)['src'])

    blue_bird_events['picLink'] = pics[:len(blue_bird_events)]

    def splitArtists(row):
        return [x.strip() for x in re.split('/', row) if (x is not None) & (x != '')]

    blue_bird_events['FiltArtist'] = blue_bird_events['Artist'].map(splitArtists)


    return blue_bird_events

def ogden_scrape():

    ogden = requests.get('https://www.ogdentheatre.com/events', proxies = proxies, verify=False)
    ogden_soup = BeautifulSoup(ogden.text, 'html.parser')
    ogden_events = list(zip(list(filter(None, [z.strip() for z in [y[0] for y in [x.contents for x in ogden_soup.findAll('a', {'title':"More Info"})]]])),[x.contents[2].strip() for x in ogden_soup.findAll('span', {'class':"date"})],[x['href'] for x in ogden_soup.findAll('a',{'class':'btn-tickets accentBackground widgetBorderColor secondaryColor tickets status_1'})]))
    ogden_events = pd.DataFrame(ogden_events)
    ogden_events['Venue'] = 'Ogden'
    ogden_events.columns = ['Artist','Date','Link','Venue']

    pics = []
    thumbs = ogden_soup.findAll('div', {'class':'thumb'})
    for x in thumbs:
        pics.append(x.find('img', alt=True)['src'])

    ogden_events['picLink'] = pics[:len(ogden_events)]


    def splitArtists(row):
        return [x.strip() for x in re.split('Presents|:', row) if (x is not None) & (x != '')]

    ogden_events['FiltArtist'] = ogden_events['Artist'].map(splitArtists)

    return ogden_events

def first_bank_scrape():

    first_bank = requests.get('https://www.1stbankcenter.com/events', proxies = proxies, verify=False)
    bank_soup = BeautifulSoup(first_bank.text, 'html.parser')
    first_bank_events = list(zip(list(filter(None, [z.strip() for z in [y[0] for y in [x.contents for x in bank_soup.findAll('a', {'title':"More Info"})]]])),[x.contents[2].strip() for x in bank_soup.findAll('span', {'class':"date"})],[x['href'] for x in bank_soup.findAll('a',{'class':'btn-tickets accentBackground widgetBorderColor secondaryColor tickets status_1'})]))
    first_bank = pd.DataFrame(first_bank_events)
    first_bank['Venue'] = '1st Bank'
    first_bank.columns = ['Artist','Date','Link','Venue']

    pics = []
    thumbs = bank_soup.findAll('div', {'class':'thumb'})
    for x in thumbs:
        pics.append(x.find('img', alt=True)['src'])

    first_bank['picLink'] = pics[:len(first_bank)]

    def splitArtists(row):
        return [x.strip() for x in re.split('/', row) if (x is not None) & (x != '')]

    first_bank['FiltArtist'] = first_bank['Artist'].map(splitArtists)

    return first_bank

def fox_scrape():

    fox = requests.get('https://www.foxtheatre.com/calendar/', proxies = proxies, verify = False)
    fox_soup = BeautifulSoup(fox.text, 'html.parser')
    calendar = fox_soup.select('td[class*="data "]')

    events = []
    for event in calendar:
            try:
                date = [event.find('span', {'class':'value-title'})['title']]
                eventTitle = [x['alt'] for x in event.findAll('img')]
                eventLinks = ['https://foxtheatre.com' + x['href'] for x in event.findAll('a', {'class':'image-url'})]
                picLinks = [x['src'] for x in event.findAll('img')]

                eventInfo = list(zip(eventTitle, eventLinks, picLinks))
                for show in eventInfo:
                    event = tuple(date) + show
                    events.append(event)
            except:
                #there is no show that day
                pass
    fox_frame = pd.DataFrame(events)
    fox_frame.columns = ['Date','Artist','Link','picLink']
    fox_frame['Venue'] = 'Fox Theatre'

    def splitArtists(row):
        return [x.strip() for x in re.split('FEAT.|-|\\+|\\(|with', row) if (x is not None) & (x != '')]

    fox_frame['FiltArtist'] = fox_frame['Artist'].map(splitArtists)

    fox_frame = fox_frame[['Artist','Date','FiltArtist','Link','Venue','picLink']]


    return fox_frame

def cervantes_scrape():

    cervantes = requests.get('https://www.cervantesmasterpiece.com/calendar/', proxies = proxies, verify=False)
    cerv_soup = BeautifulSoup(cervantes.text, 'html.parser')
    calendar = cerv_soup.select('td[class*="data "]')

    events = []
    for event in calendar:
            try:
                date = [event.find('span', {'class':'value-title'})['title']]
                eventTitle = [x['alt'] for x in event.findAll('img')]
                eventLinks = ['https://www.cervantesmasterpiece.com' + x['href'] for x in event.findAll('a', {'class':'image-url'})]
                picLinks = [x['src'] for x in event.findAll('img')]

                eventInfo = list(zip(eventTitle, eventLinks, picLinks))
                for show in eventInfo:
                    event = tuple(date) + show
                    events.append(event)
            except:
                #there is no show that day
                pass

    cervantes_event_frame = pd.DataFrame(events)
    cervantes_event_frame.columns = ['Date','Artist','Link','picLink']
    cervantes_event_frame['Venue'] = 'Presented by Cervantes - check site'

    def remove_at(row):
        try:
            return row.split('@')[0]
        except:
            return row

    cervantes_event_frame['Artist'] = cervantes_event_frame['Artist'].map(remove_at)
    cervParse = cervantes_event_frame

    cervParse['EventTitle'] = cervParse['Artist'].map(lambda x : ' , '.join(x.split(',')))
    cervParse['EventTitle'] = cervParse['Artist'].map(lambda x : ' ( '.join(x.split('(')))
    cervParse['EventTitle'] = cervParse['Artist'].map(lambda x : ' ) '.join(x.split(')')))



    def POStag(row):
        data = []
        tokens = [event for event in row.split()]
        # for every token in the event title, tag with a Part Of Speach label from the NLTK library
        tagged = nltk.pos_tag(tokens)

        return tagged


    cervParse['POS'] = cervParse.EventTitle.map(POStag)


    #     cervParse['POS'] = cervParse['labeled'].map(POStags)
    #     cervParse['POS'] = cervParse['POS'].map(lambda x : x[0])

    #     data = cervParse['POS'].tolist()

    def word2features(doc, i):

        # create features for training based on charchteristics of the word, and its surrounding words/ charachters

        word = doc[i][0]
        postag = doc[i][1]
        features = [
            'word.lower=' + word.lower(),
            'bias',
            'postag=' + postag,
            'word.isupper=%s' % word.isupper(),
            'word.istitle=%s' % word.istitle()
        ]

        if i > 0:
            word1 = doc[i-1][0]
            postag1 = doc[i-1][1]
            features.extend([
                '-1:word.lower=' + word1.lower(),
                '-1:postag=' + postag1,
                '-1:postag[:2]=' + postag1[:2],
                '-1:word.istitle=%s' % word1.istitle(),
                '-1:word.isupper=%s' % word1.isupper()

            ])

        else:
            features.append('BOS')

        if i < len(doc)-1:
            word1 = doc[i+1][0]
            postag1 = doc[i+1][1]
            features.extend([
                '+1:word.lower=' + word1.lower(),
                '+1:postag=' + postag1,
                '+1:postag[:2]=' + postag1[:2],
                '+1:word.istitle=%s' % word1.istitle(),
                '+1:word.isupper=%s' % word1.isupper()
            ])
        else:
            features.append('EOS')

        return features

    # A function for extracting features in documents
    def extract_features(doc):
        return [word2features(doc, i) for i in range(len(doc))]

    # A function fo generating the list of labels for each document
    def get_labels(doc):
        return [label for (token, postag, label) in doc]

    cervParse['features'] = cervParse['POS'].map(lambda x : extract_features(x))
    # extract features for every event title

    cervParse['preds'] = cervParse.features.map(lambda x : [tagger.tag(x)])
    # map trained model the features of each event title
    cervParse['toks'] = cervParse.POS.map(lambda x: [x[0] for x in x])
    # get prediction tags and word tokens into lists so they can be combined

    def zipper(row):
        return [(x,y) for x,y in list(zip(row.toks, row.preds[0])) if y in ['A','S']]

    cervParse['artistPreds'] = cervParse.apply(zipper, axis=1)

    def combine(row):
        results = [(x,y) for x,y in list(zip(row.toks, row.preds[0])) if y in ['A','S']]
        artist = []
        for x in results:
            if x[1] == 'A':
                artist.append(x[0])
            else:
                artist.append(',')
        return ', '.join([y for y in [x.strip() for x in ' '.join(artist).split(',')] if y != ''])

    cervParse['predictions'] = cervParse.apply(combine, axis=1)
    cervParse['predictions'] = cervParse['predictions'].map(lambda x : [x for x in x.split(',')])
    cervArtistFrame = cervParse[['Artist','predictions','Date','Venue','Link','picLink']]
    cervArtistFrame.columns = ['Artist','FiltArtist','Date','Venue','Link','picLink']
    cervArtistFrame = cervArtistFrame[cervArtistFrame['Venue'] != 'RED ROCKS AMPHITHEATRE']

    cervArtistFrame = cervArtistFrame[['Artist','Date','FiltArtist','Link','Venue','picLink']]

    return cervArtistFrame

def vinyl_scrape():

    BASE_URL = 'http://coclubs.com/club-vinyl/event-calendar/'

    with requests.Session() as session:
        response = session.get(BASE_URL, proxies = proxies, verify=False)
        soup = BeautifulSoup(response.content, 'html.parser')

        for frame in soup.select("iframe"):
            frame_url = urljoin(BASE_URL, frame["src"])

            response = session.get(frame_url, proxies = proxies, verify=False)
            frame_soup = BeautifulSoup(response.content, 'html.parser')

    try:
        vinyl_var = frame_soup.findAll('script')[13].string
    except:
        vinyl_var = frame_soup.findAll('script')[12].string

    json_string = vinyl_var.split('var organizationEvents = ')[1].split(';\n')[0]
    vinyl_loads = json.loads(json_string)

    vinyl_artists = []

    for x in vinyl_loads:
        events = (x['title'], x['start_time'],x['url'], x['poster_url']['small'])
        vinyl_artists.append(events)

    vinyl_frame = pd.DataFrame(vinyl_artists)
    vinyl_frame['Venue'] = 'Club Vinyl'
    vinyl_frame.columns = ['Artist','Date','Link','picLink','Venue']

    def splitArtists(row):
        return [x.strip() for x in re.split('BASS OPS:|\\+|:|at|-', row) if (x is not None) & (x != '')]

    vinyl_frame['FiltArtist'] = vinyl_frame['Artist'].map(splitArtists)

    return vinyl_frame

def gothic_scrape():

    gothic = requests.get('https://www.gothictheatre.com/events', proxies = proxies, verify=False)
    gothic_soup = BeautifulSoup(gothic.text, 'html.parser')
    gothic_events = gothic_soup.findAll('body', {'id':"events_axs"})

    gothic_artists = []
    gothic_dates = []
    links = []

    for x in gothic_events[0].findAll('a',{'title':"More Info"}):
        gothic_artists.append(x.contents[0].strip())

    for x in gothic_events[0].findAll('span',{'class':"date"}):
        gothic_dates.append(x.contents[2].strip())

    for x in gothic_events[0].findAll('a',{'class':'btn-tickets accentBackground widgetBorderColor secondaryColor tickets status_1'}):
        links.append(x['href'])

    gothic_artists = list(filter(None, gothic_artists))

    gothic_frame = pd.DataFrame(list(zip(gothic_artists, gothic_dates,links)))
    gothic_frame['Venue'] = 'Gothic'
    gothic_frame.columns = ['Artist','Date','Link','Venue']

    pics = []
    thumbs = gothic_soup.findAll('div', {'class':'thumb'})
    for x in thumbs:
        pics.append(x.find('img', alt=True)['src'])

    gothic_frame['picLink'] = pics[:len(gothic_frame)]

    def splitArtists(row):
        return [x.strip() for x in re.split('Feat.|\s/', row) if (x is not None) & (x != '')]

    gothic_frame['FiltArtist'] = gothic_frame['Artist'].map(splitArtists)


    return gothic_frame

def church_scrape():
    BASE_URL = 'http://coclubs.com/the-church/event-calendar/'

    with requests.Session() as session:
        response = session.get(BASE_URL, proxies = proxies, verify=False)
        soup = BeautifulSoup(response.content, 'html.parser')

        for frame in soup.select("iframe"):
            frame_url = urljoin(BASE_URL, frame["src"])

            response = session.get(frame_url, proxies = proxies, verify=False)
            frame_soup = BeautifulSoup(response.content, 'html.parser')

    try:
        church_var = frame_soup.findAll('script')[13].string
    except:
        church_var = frame_soup.findAll('script')[12].string
    json_string = church_var.split('var organizationEvents = ')[1].split(';')[0]
    church_loads = json.loads(json_string)

    church_artists = []

    for x in church_loads:
        events = (x['title'], x['start_time'],x['url'],x['poster_url']['small'])
        church_artists.append(events)


    church_frame = pd.DataFrame(church_artists)
    church_frame['Venue'] = 'Church'
    church_frame.columns = ['Artist','Date','Link','picLink','Venue']

    def splitArtists(row):
        return [x.strip() for x in re.split(':', row) if (x is not None) & (x != '')]

    church_frame['FiltArtist'] = church_frame['Artist'].map(splitArtists)


    return church_frame

def summitScrape():

    today = datetime.today().strftime('%Y-%m-%d')
    response = requests.get('http://www.summitdenver.com/api/EventCalendar/GetEvents?startDate={}&endDate=2020-11-01&venueIds=37697043,38999257&limit=200&offset=1&genre=&artist=&offerType=STANDARD&useTMOnly=false&useEventBrite=false'.format(today), proxies = proxies, verify=False)

    summitResponse = response.json()
    summitJson = json.loads(summitResponse)

    summitInfo = []
    for x in summitJson['result']:
        artists = x['title']
        dates = x['eventDate'].split(' ')[0]
        venue = x['venueName']
        link = x['ticketUrl']
        picLink = x['eventImageLocation']
        info = (artists, dates,link, venue,picLink)
        summitInfo.append(info)

    summitFrame = pd.DataFrame(summitInfo)
    summitFrame.columns = ['Artist','Date','Link','Venue','picLink']
    def splitArtists(row):
        return [x.strip() for x in re.split(',|:|\*|-|\+|featuring', row) if (x is not None) & (x != '')]
    summitFrame['FiltArtist'] = summitFrame['Artist'].map(splitArtists)

    return summitFrame

def bellyScrape():

    belly = requests.get('https://bellyupaspen.com/calendar/event-listing', proxies = proxies, verify = False)
    bellySoup = BeautifulSoup(belly.text, 'html.parser')

    divs = bellySoup.findAll('div', {'class':'item itemPreview hasImg onsale itemInnerContent frontgateFeedItem frontgateUpcomingItem'})

    bellyInfo = []

    for x in divs:
        date = x.find('span', {'class':'eventWeekday'}).text + x.find('span', {'class':'eventMonth'}).text + ' ' +  x.find('span', {'class':'eventDay'}).text
        artist = x.find('h2').text
        link = x.find('a', {'class':'button buyButton'})['href']
        pic = x.find('div', {'class':'frontgateFeedImg'}).find('img', alt=True)['src']
        info = (artist, date, link, pic)
        bellyInfo.append(info)

    bellyFrame = pd.DataFrame(bellyInfo)
    bellyFrame.columns = ['Artist','Date','Link','picLink']
    bellyFrame['Venue'] = 'Belly-Up Aspen'

    def splitArtists(row):
        return [x.strip() for x in re.split(':|ft.|with|;|of', row) if (x is not None) & (x != '')]

    bellyFrame['FiltArtist'] = bellyFrame['Artist'].map(splitArtists)

    return bellyFrame

def larimer_scrape():

    larimer = requests.get('https://www.larimerlounge.com/calendar/', proxies=proxies, verify=False)
    larimer_soup = BeautifulSoup(larimer.text, 'html.parser')

    tableRows = larimer_soup.findAll('td')

    events = []
    for row in tableRows:
        try:
            date = row.find('span', {'class':'value-title'})['title']
            eventTitle = row.find('img')['alt']
            eventLinks = 'https://www.larimerlounge.com' + row.find('a', href=True)['href']
            picLinks = row.find('img')['src']
            venue = 'Larimer Lounge'

            eventInfo = (date, eventTitle, eventLinks, picLinks, venue)
            events.append(eventInfo)
        except:
            pass

    larimer_frame = pd.DataFrame(events)
    larimer_frame.columns = ['Date','Artist','Link','picLink','Venue']


    def splitArtists(row):
        return [x.strip() for x in re.split('\\+|\(ft.|\(|\)|/|Feat.|&', row) if (x is not None) & (x != '')]

    larimer_frame['FiltArtist'] = larimer_frame['Artist'].map(splitArtists)

    larimer_frame = larimer_frame[['Artist','Date','FiltArtist','Link','Venue','picLink']]

    return larimer_frame

def missionScrape():

    mission = requests.get('https://missionballroom.com/data/events-index.json', proxies = proxies, verify = False)
    mission = mission.json()

    missionData = []
    for x in mission:
        date = x['date']
        artist1 = x['title']
        artist2 = x['subtitle']
        pic = x['img318x187']
        if artist2 == '':
            artist = artist1
        else:
            artist = artist1 + ',' + artist2

        venue = 'Mission Ballroom'
        ticketLink = x['tickets']
        info = (artist, date, ticketLink, venue,pic)
        missionData.append(info)

    missionFrame = pd.DataFrame(missionData)
    missionFrame.columns = ['Artist','Date','Link','Venue','picLink']


    def splitArtists(row):
        return [x.strip() for x in re.split('&|WITH|,|/|with', row) if x not in ['',' ']]
    missionFrame['FiltArtist'] = missionFrame['Artist'].map(splitArtists)

    return missionFrame



functions = ['missionScrape()','cervantes_scrape()','temple_scrape()', 'fillmore_scrape()','black_box_scrape()','blue_bird_scrape()','ogden_scrape()','first_bank_scrape()','vinyl_scrape()','gothic_scrape()','church_scrape()','redRocks()','fox_scrape()','larimer_scrape()','summitScrape()','bellyScrape()','mishScrape()']


result = []
rang = range(0,len(functions))

def sf_query(run):
            try:
                result.append(eval(functions[run]))
            except:
                web_hook = 'https://hooks.slack.com/services/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'
                slack_msg = {'text':f'There was an error scrapiing (API) -- {functions[run]}'}
                requests.post(web_hook, data = json.dumps(slack_msg))
                pass

def main_2():
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        results = [executor.submit(sf_query,run) for run in rang]


def scrapeVenues():
    execute = main_2()
    denver_concerts = pd.concat(result)
    denver_concerts['Date'] = denver_concerts['Date'].map(lambda x : parse(x).date())
    denver_concerts.columns = ['artist','date','filtArtist','link','venue','picLink']

    return denver_concerts


concertDict = defaultdict(list)
def eventDict():
    denver_concerts = scrapeVenues()
    for artists in denver_concerts.values:
        for artist in artists[2]:
            if artist.strip().upper() == '':
                pass
            else:
                concertDict[artist.strip().upper()].append(artists[0])
                concertDict[artist.strip().upper()].append(artists[1])
                concertDict[artist.strip().upper()].append(artists[4])
                concertDict[artist.strip().upper()].append(artists[3])
                concertDict[artist.strip().upper()].append(artists[5])
    return denver_concerts


def findMatches(user):

    denver_concerts = eventDict()
    userLikes = mapFilters(user)

    matchResults = []
    for x in userLikes[0]:
        try:
            shows = concertDict[x]
            if len(shows) > 0:
                occurance = (int(len(shows))/5)
                for y in range(0, int(occurance)):
                    n = y*5
                    vals = [shows[n],shows[n+1], shows[n+2],x, shows[n+3], shows[n+4]]
                    matchResults.append(vals)
        except:
            pass


    matches = pd.DataFrame(matchResults)
    matches.columns = ['Artist','Date','Venue','Caused_By','Link','picLink']
    matches = matches.drop_duplicates()
    matches = matches.groupby(['Artist', 'Date','Venue','Link','picLink']).agg({'Caused_By': lambda x: ', '.join(x)}).sort_values('Date').reset_index()
    matches = matches[['Link','Date','Venue','Caused_By','picLink']]
    matches['Date'] = matches.Date.map(lambda x : str(x) + '-22')
    matches['Date'] = pd.to_datetime(matches['Date'])
    matches['Date'] = matches.Date.map(lambda x : x.strftime('%Y-%m-%dT%H:%M:%S.%f%SZ'))
    matches = matches.drop_duplicates()

    art = []
    for row in matches['Caused_By']:
        try:
            for artist in row.split(','):
                art.append(artist.strip())
        except:
            pass

    artMatches = list(set(art))
    # countFrame = userLikes[1]
    # countFrame = countFrame[countFrame['Artist'].isin(artMatches)]
    # countFrame = countFrame.sort_values('like_count', ascending=False).drop_duplicates('Artist').sort_values('size', ascending=False)
    # countFrame['like_count'] = countFrame['like_count'].map('{:,.0f}'.format)


    matches.columns = ['event','date','venue','likedArtists', 'picLink']
    jsonMatches = matches.to_dict('records')


    return [jsonMatches]
