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
# from urllib.parse import urljoin
import warnings
warnings.filterwarnings("ignore")
import nltk
import re
from concurrent import futures
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

tagger = pycrfsuite.Tagger()
tagger.open(r'C:\Users\schla\Desktop\concertFinderDjango\concert\home\scripts\crf.model')


proxies = {
}

def getUser(user):

    userInfo = requests.get('https://soundcloud.com/{}/'.format(user), proxies=proxies, verify=False)
    userSoup = BeautifulSoup(userInfo.text, 'html.parser')
    userId = str(userSoup).split('https://api.soundcloud.com/users/')[1].split('"')[0]
    return userId


# In[125]:


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

    userId = getUser(user)
    nextHref = 'https://api-v2.soundcloud.com/users/{}/likes'.format(userId)

    tracks = []
    playlists = []

    while nextHref != None:

        response = requests.get(nextHref, headers=headers, params=params, proxies=proxies, verify=False)
        jsonLikes = json.loads(response.text)

        userTracks = [x['track'] for x in jsonLikes['collection'] if 'track' in list(x.keys())]
        trackLabels = ['title','user','label_name','publisher_metadata','tag_list','genre','kind','permalink_url','likes_count']

        trackData = []
        for x in userTracks:
            trackInfo = [x.get(label) for label in trackLabels]
            trackData.append(trackInfo)

        userPlaylists = [y['permalink_url'] for y in [x['playlist'] for x in jsonLikes['collection'] if 'playlist' in list(x.keys())]]

        tracks.append(trackData)
        playlists.append(userPlaylists)

        nextHref = jsonLikes['next_href']


    return tracks


def getArtist(row):
    try:
        return row.get('artist')
    except:
        return

def getUsername(row):
    try:
        return row.get('username')
    except:
        return

def makeFrame(user):
    tracks = userLikes(user)

    likeFrame = pd.concat(pd.DataFrame(tracks[x]) for x in range(0, len(tracks)))
    likeFrame.columns = ['title','user','label_name','publisher_metadata','tag_list','genre','kind','song_url','likes_count']
    likeFrame['artist'] = likeFrame.publisher_metadata.map(getArtist)
    likeFrame['username'] = likeFrame.user.map(getUsername)
    likeFrame = likeFrame[['title','user','label_name','tag_list','genre','kind','artist','username','song_url','likes_count']]
    likeFrame['title_user'] = likeFrame['title'] + ' % '  + likeFrame['username']
    testData = likeFrame[['title_user','artist','song_url','likes_count']]

    return testData

# many artists have emoji's in thier names or song titles, removing as they dont add any value just headache

def removeEmoji(row):
    return row.encode('ascii', 'ignore').decode('ascii').strip()

def filtData(user):
    testData = makeFrame(user)

    testData.title_user = testData.title_user.map(removeEmoji)
    testData['title_user'] = testData['title_user'].map(lambda x : x.upper())
    testData['artist'] = testData['artist'].map(lambda x :x if pd.isnull(x) else x.upper())

    return testData

def findRemix(x):

    if (str(x).count('(') == 1) & (' REMIX' in x):
        return x[x.find("(")+1:x.find("REMIX")]
    elif (str(x).count('[') == 1) & (' REMIX' in x):
        return x[x.find("[")+1:x.find("REMIX")]
    elif (str(x).count('(') == 1) & (' RMX' in x):
        return x[x.find("(")+1:x.find(" RMX")]
    elif (str(x).count('[') == 1) & (' RMX' in x):
        return x[x.find("[")+1:x.find(" RMX")]
    elif (str(x).count('(') == 1) & (' VIP)' in x):
        return x[x.find("(")+1:x[0].find(" VIP)")]
    elif (str(x).count('[') == 1) & (' VIP)' in x):
        return x[x.find("[")+1:x[0].find(" VIP)")]
    elif (str(x).count('(') == 1) & (' FLIP' in x):
        return x[x.find("(")+1:x.find(" FLIP")]
    elif (str(x).count('[') == 1) & (' FLIP' in x):
        return x[x.find("[")+1:x.find(" FLIP")]
    else:
        return

# For electronic artists, I am personally more interested in the artist who made the remix and not the original artist

def artistSong(row):

    # if there is only 1 '-' in the string and this -
    # comes BEFORE the % username, typically what is to the left of the dash is the artist
    # also strings with FT (featured artists) typicall follow the logic "song name (ft artist) - Artist"..
    # artists are found on the oposite side of the '-' where 'FEAT/FT lies'

    if row.trueArtists is None:
        if (row.title_user.count('-') == 1) & (row.title_user.find('-') < row.title_user.find('%')):
            if 'FT.' in row.title_user:
                if row.title_user.find('FT.') - row.title_user.find('-') > 0:
                    return row.title_user.split('-')[0].split('%')[0].strip()
                    print(row.title_user)
                else:
                    return row.title_user.split('-')[1].split('%')[0].strip()
                    print(row.title_user, row.title_user.split('-')[1].split('%')[0].strip())

            if 'FEAT' in row.title_user:
                if row.title_user.find('FEAT') - row.title_user.find('-') > 0:
                    return row.title_user.split('-')[0].split('%')[0].strip()
                    print(row.title_user)
                else:
                    return row.title_user.split('-')[1].split('%')[0].strip()
                    print(row.title_user, row.title_user.split('-')[1].split('%')[0].strip())
            else:
                return row.title_user.split('-')[0].strip()
        else:
            return row.trueArtists
    else:
        return row.trueArtists

def artistMatch(row):
    # if the username matches the artist name, I am confident that is the Artist
    if row.trueArtists is None:
        # don't overwrite existing artist extractions
        user = row.title_user.split('%')[1].strip()
        if user == row.artist:
            return row.artist
        else:
            return row.trueArtists
    else:
        return row.trueArtists

# a lot of times, if a track or a mix is reposted by a label, or radio station etc it can be very hard to tell the artist name, lets just omit these
# otherwise, its a good bet the username is actually the artist name as well
def omitReposts(row):
    if row.trueArtists is None:
        counter = 0
        for x in ['MIX','RECORDS','RADIO','LABEL','COLLECTIVE','GUEST','RECORDINGS','RECORD','[',']','(',')']:
            if x in row.title_user:
                counter += 1
            else:
                continue
        if (row.trueArtists is None) & (counter == 0):
            return row.title_user.split('%')[1].strip()
        else:
            return row.trueArtists
    else:
        return row.trueArtists

# split multiple artists into lists of individual artists
def splitArtists(row):
    if row is not None:
        return re.split('\s+&+\s|\s+X+\s|\s+x+\s|,', row)

def mapFilters(user):

    testData = filtData(user)

    testData['trueArtists'] = testData.title_user.map(findRemix)
    testData['trueArtists'] = testData.apply(artistSong, axis=1)
    testData['trueArtists'] = testData.apply(artistMatch, axis=1)
    testData['trueArtists'] = testData.apply(omitReposts, axis=1)
    testData['trueArtistsSplit'] = testData['trueArtists'].map(splitArtists)

    indvArtists = []
    for row in testData.values:
        try:
            for artist in row[5]:
                info = (artist.strip().upper(), row[3], row[2])
                indvArtists.append(info)
        except:
            pass


    songCounts = pd.DataFrame(indvArtists)
    songCounts.columns = ['Artist','like_count','song_url']
    songCounts['size'] = songCounts.groupby('Artist')['Artist'].transform('size')


    userLikes = []
    for x in testData['trueArtistsSplit']:
        if x is None:
            pass
        elif len(str(x)) >2:
            for y in x:
                userLikes.append(y)

    return [list(set(userLikes)),songCounts]



# In[126]:


def temple_scrape():

    temple_events = requests.get('https://www.templedenver.com/event-calendar/all/', proxies = proxies, verify=False)
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

        descSoup = BeautifulSoup(desc, 'html.parser')
        try:
            info = descSoup.find('span', {'style':'font-size: 18pt; color: #ff6600;'}).text
        except:
            try:
                info = descSoup.find('span', {'style':'color: #ff6600;'}).text
            except:
                info = headline

        res = (info, date,link, 'Mishawaka Amphitheatre')
        mishData.append(res)

    mishFrame = pd.DataFrame(mishData)
    mishFrame.columns = ['Artist','Date','Link','Venue']
    mishFrame['Date'] = mishFrame['Date'].map(lambda x : x.split('T')[0])

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
        data = (y['name'],y['startDate'].split('T')[0],y['offers']['url'], 'Fillmore Auditorium')
        filmore_events.append(data)

    filmore_events_frame = pd.DataFrame(filmore_events)
    filmore_events_frame.columns = ['Artist','Date','Link','Venue']

    def splitArtists(row):
        return [x.strip() for x in re.split(';|:|,', row) if x != ' ']
    filmore_events_frame['FiltArtist'] = filmore_events_frame['Artist'].map(splitArtists)

    return filmore_events_frame

def redRocks():
    redRocks = requests.get('https://www.redrocksonline.com/events/category/Concerts', proxies = proxies, verify=False)
    redSoup = BeautifulSoup(redRocks.text, 'html.parser')
    redInfo = redSoup.findAll('div', {'class':'m-info-container'})

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

    redRocksFrame = pd.DataFrame(details)
    redRocksFrame.columns = ['Artist','Date','Link']
    redRocksFrame['Venue'] = 'Red Rocks'

    def splitArtists(row):
        return [x.strip() for x in re.split(';|:|,|,with|/|special guest|with|-', row) if x != '']

    redRocksFrame['FiltArtist'] = redRocksFrame['Artist'].map(splitArtists)

    return redRocksFrame

def black_box_scrape():

    black_box = requests.get('https://blackboxdenver.ticketfly.com/', proxies = proxies, verify=False)
    black_soup = BeautifulSoup(black_box.text, 'html.parser')
    black_event = black_soup.findAll('div', {'class':'list-view-details vevent'})

    black_box_events = []
    links = []
    for x in black_event:
        headliner = x.findAll('a')[0].contents[0]
        try:
            support = x.findAll('h2', {'class', 'supports description'})[0].text
            artists = headliner + ',' + support
            eventData = (artists, (x.findAll('h2',{'class':'dates'}))[0].contents[0], 'Black Box')
        except:
            artists = headliner
            eventData = (artists, (x.findAll('h2',{'class':'dates'}))[0].contents[0], 'Black Box')
        try:
            link = 'https://www.ticketfly.com/purchase' + x.find('a', href=True)['href']
            links.append(link)
        except:
            link = 'tickets at the door'
            links.append(links)

        black_box_events.append(eventData)


    black_box_events = pd.DataFrame(black_box_events)
    black_box_events.columns = ['Artist','Date','Venue']
    black_box_link = pd.DataFrame(links)
    black_box_events = pd.concat([black_box_events,black_box_link], axis=1)
    black_box_events.columns = ['Artist','Date','Venue','Link']
    black_box_events = black_box_events[['Artist','Date','Link','Venue']]


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


    def splitArtists(row):
        return [x.strip() for x in re.split('Presents', row) if (x is not None) & (x != '')]

    ogden_events['FiltArtist'] = ogden_events['Artist'].map(splitArtists)

    return ogden_events

def first_bank_scrape():

    first_bank = requests.get('https://www.1stbankcenter.com/events', proxies = proxies, verify=False)
    bank_soup = BeautifulSoup(first_bank.text, 'html.parser')
    first_bank_events = list(zip(list(filter(None, [z.strip() for z in [y[0] for y in [x.contents for x in bank_soup.findAll('a', {'title':"More Info"})]]])),[x.contents[2].strip() for x in bank_soup.findAll('span', {'class':"date"})],[x['href'] for x in bank_soup.findAll('a',{'class':'btn-tickets accentBackground widgetBorderColor secondaryColor tickets status_1'})]))
    first_bank = pd.DataFrame(first_bank_events)
    first_bank['Venue'] = '1st Bank'
    first_bank.columns = ['Artist','Date','Link','Venue']

    def splitArtists(row):
        return [x.strip() for x in re.split('/', row) if (x is not None) & (x != '')]

    first_bank['FiltArtist'] = first_bank['Artist'].map(splitArtists)

    return first_bank

def fox_scrape():
    fox = requests.get('https://www.foxtheatre.com/calendar/', proxies = proxies, verify = False)
    fox_soup = BeautifulSoup(fox.text, 'html.parser')
    fox_calender = fox_soup.findAll('td')

    fox_artists = []
    for x in fox_calender:
        artist = "\n".join([img['alt'] for img in x.find_all('img', alt=True)])
        fox_artists.append(artist)

    fox_dates = []
    for index, x in enumerate(fox_calender):
        date = x.findAll('span', {'class':"value-title"})
        try:
            fox_dates.append((index, str(date).split('<span class="value-title" title="')[1].split('T')[0]))
        except:
            pass

    artists = pd.DataFrame(fox_artists).reset_index()
    dates = pd.DataFrame(fox_dates)
    fox_info = dates.merge(artists, left_on=0, right_on='index', how='inner')
    fox_info = fox_info[[1,'0_y']]
    fox_info['Venue'] = 'Fox Theatre'

    fox_frame = fox_info[fox_info['0_y'] != '']
    fox_frame.columns = ['Date','Artist','Venue']
    fox_frame = fox_frame[['Artist','Date','Venue']]

    def splitArtists(row):
        return [x.strip() for x in re.split('FEAT.|-|\\+|\\(', row) if (x is not None) & (x != '')]

    fox_frame['FiltArtist'] = fox_frame['Artist'].map(splitArtists)

    fox_frame['Link'] = 'No Link at this time, sorry!'
    fox_frame = fox_frame[['Artist','Date','FiltArtist','Link','Venue']]


    return fox_frame

def cervantes_scrape():

    cervantes = requests.get('https://www.cervantesmasterpiece.com/calendar/', proxies = proxies, verify=False)
    cerv_soup = BeautifulSoup(cervantes.text, 'html.parser')
    cerv_calender = cerv_soup.findAll('td')

    cerv_artists = []
    for x in cerv_calender:
        cerv_artists.append(x.findAll('a', {'class':'url'}))

    cerv_artist_content = []
    for index, x in enumerate(cerv_artists):
        try:
            for y in range(0, len(x)):
                link = 'https://www.cervantesmasterpiece.com' + x[0]['href']
                arts = (index, x[y].contents[0], link)
                cerv_artist_content.append(arts)
        except:
            pass

    cerv_dates = []
    for x in cerv_calender:
        cerv_dates.append(x.findAll('span', {'class':"value-title"}))

    cerv_dates_clean = []

    for index, x in enumerate(cerv_dates):
        try:
            dates = (index, str(x[0]).split('<span class="value-title" title="')[1].split('T')[0])
            cerv_dates_clean.append(dates)
        except:
            pass

    cerv_date_frame = pd.DataFrame(cerv_dates_clean)
    cerv_artist_frame = pd.DataFrame(cerv_artist_content)
    cerv_date_frame.columns = ['index','Date']
    cerv_artist_frame.columns = ['index','Artist','Link']

    cervantes_event_frame = cerv_date_frame.merge(cerv_artist_frame, left_on='index', right_on='index', how='inner')
    cervantes_event_frame = cervantes_event_frame[['Artist','Date','Link']]

    def event_loc(row):
        if '@' in row:
            return row.split('@')[1]
        else:
            return 'Cervantes Masterpiece'

    cervantes_event_frame['Venue'] = cervantes_event_frame['Artist'].map(event_loc)

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
    cervArtistFrame = cervParse[['Artist','predictions','Date','Venue','Link']]
    cervArtistFrame.columns = ['Artist','FiltArtist','Date','Venue','Link']
    cervArtistFrame = cervArtistFrame[cervArtistFrame['Venue'] != 'RED ROCKS AMPHITHEATRE']

    cervArtistFrame = cervArtistFrame[['Artist','Date','FiltArtist','Link','Venue']]

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
        events = (x['title'], x['start_time'],x['url'])
        vinyl_artists.append(events)

    vinyl_frame = pd.DataFrame(vinyl_artists)
    vinyl_frame['Venue'] = 'Club Vinyl'
    vinyl_frame.columns = ['Artist','Date','Link','Venue']

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
        events = (x['title'], x['start_time'],x['url'])
        church_artists.append(events)


    church_frame = pd.DataFrame(church_artists)
    church_frame['Venue'] = 'Church'
    church_frame.columns = ['Artist','Date','Link','Venue']

    def splitArtists(row):
        return [x.strip() for x in re.split(':', row) if (x is not None) & (x != '')]

    church_frame['FiltArtist'] = church_frame['Artist'].map(splitArtists)


    return church_frame

def summitScrape():

    today = datetime.today().strftime('%Y-%m-%d')
    response = requests.get('http://www.summitdenver.com/api/EventCalendar/GetEvents?startDate={}&endDate=2019-12-31&venueIds=5289&limit=200&offset=1&genre=&artist=&offerType=&useTMOnly=false'.format(today), proxies = proxies, verify=False)

    summitResponse = response.json()
    summitJson = json.loads(summitResponse)

    summitInfo = []
    for x in summitJson['tfResult']['events']:
        artists = x['headlinersName'] + ',' + x['supportsName']
        dates = x['startDate'].split(' ')[0]
        venue = x['venue']['name']
        link = x['ticketPurchaseUrl']
        info = (artists, dates,link, venue)
        summitInfo.append(info)

    summitFrame = pd.DataFrame(summitInfo)
    summitFrame.columns = ['Artist','Date','Link','Venue']
    def splitArtists(row):
        return [x.strip() for x in re.split(',|:', row) if (x is not None) & (x != '')]
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
        info = (artist, date, link)
        bellyInfo.append(info)

    bellyFrame = pd.DataFrame(bellyInfo)
    bellyFrame.columns = ['Artist','Date','Link']
    bellyFrame['Venue'] = 'Belly-Up Aspen'

    def splitArtists(row):
        return [x.strip() for x in re.split(':|ft.|with|;|of', row) if (x is not None) & (x != '')]

    bellyFrame['FiltArtist'] = bellyFrame['Artist'].map(splitArtists)

    return bellyFrame

def larimer_scrape():

    larimer = requests.get('https://www.larimerlounge.com/calendar/', proxies=proxies, verify=False)
    larimer_soup = BeautifulSoup(larimer.text, 'html.parser')
    lamimer_calendar = larimer_soup.findAll('td')

    larimer_artists = []
    for x in lamimer_calendar:
        artist = "\n".join([img['alt'] for img in x.find_all('img', alt=True)])
        larimer_artists.append(artist)

    larimer_dates = []
    for index, x in enumerate(lamimer_calendar):
        date = x.findAll('span', {'class':"value-title"})
        try:
            larimer_dates.append((index, str(date).split('<span class="value-title" title="')[1].split('T')[0]))
        except:
            pass

    artists = pd.DataFrame(larimer_artists).reset_index()
    dates = pd.DataFrame(larimer_dates)
    larimer_info = dates.merge(artists, left_on=0, right_on='index', how='inner')
    larimer_info = larimer_info[[1,'0_y']]
    larimer_info['Venue'] = 'Larimer Lounge'

    larimer_frame = larimer_info[larimer_info['0_y'] != '']
    larimer_frame.columns = ['Date','Artist','Venue']
    larimer_frame = larimer_frame[['Artist','Date','Venue']]

    def splitArtists(row):
        return [x.strip() for x in re.split('\\+|\(ft.|\(|\)', row) if (x is not None) & (x != '')]

    larimer_frame['FiltArtist'] = larimer_frame['Artist'].map(splitArtists)

    larimer_frame['Link'] = 'No Link at this time, sorry!'
    larimer_frame = larimer_frame[['Artist','Date','FiltArtist','Link','Venue']]

    return larimer_frame

def missionScrape():

    mission = requests.get('https://missionballroom.com/data/events-index.json', proxies = proxies, verify = False)
    mission = mission.json()

    missionData = []
    for x in mission:
        date = x['date']
        artist1 = x['title']
        artist2 = x['subtitle']
        if artist2 == '':
            artist = artist1
        else:
            artist = artist1 + ',' + artist2

        venue = 'Mission Ballroom'
        ticketLink = x['tickets']
        info = (artist, date, ticketLink, venue)
        missionData.append(info)

    missionFrame = pd.DataFrame(missionData)
    missionFrame.columns = ['Artist','Date','Link','Venue']

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
                pass
                # print('error', functions[run])
def main_2():
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        results = [executor.submit(sf_query,run) for run in rang]


def scrapeVenues():
    execute = main_2()
    denver_concerts = pd.concat(result)
    denver_concerts['Date'] = denver_concerts['Date'].map(lambda x : parse(x).date())

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
    return denver_concerts



def findMatches(user):


    denver_concerts = eventDict()
    userLikes = mapFilters(user)

    matchResults = []
    for x in userLikes[0]:
        try:
            shows = concertDict[x]
            if len(shows) > 0:
                occurance = (int(len(shows))/4)
                for y in range(0, int(occurance)):
                    n = y*4
                    vals = [shows[n],shows[n+1], shows[n+2],x, shows[n+3]]
                    matchResults.append(vals)
        except:
            pass


    matches = pd.DataFrame(matchResults)
    matches.columns = ['Artist','Date','Venue','Caused_By','Link']
    matches = matches.drop_duplicates()
    matches = matches.groupby(['Artist', 'Date','Venue','Link']).agg({'Caused_By': lambda x: ', '.join(x)}).sort_values('Date').reset_index()

    def nameLink(row):
        if row.Link == 'No Link at this time, sorry!':
            return row.Artist
        else:
            return '<a href="{0}">{1}</a>'.format(row.Link, row.Artist)

    matches['NameLink'] = matches.apply(nameLink, axis=1)
    matches = matches[['NameLink','Date','Venue','Caused_By']]

    art = []
    for row in matches['Caused_By']:
        try:
            for artist in row.split(','):
                art.append(artist.strip())
        except:
            pass

    artMatches = list(set(art))
    countFrame = userLikes[1]
    countFrame = countFrame[countFrame['Artist'].isin(artMatches)]
    countFrame = countFrame.sort_values('like_count', ascending=False).drop_duplicates('Artist').sort_values('size', ascending=False)
    countFrame['song_url'] = countFrame['song_url'].apply(lambda x: '<a href="{0}">song_link</a>'.format(x))
    countFrame['like_count'] = countFrame['like_count'].map('{:,.0f}'.format)
    countFrame = countFrame.reset_index()
    countFrame = countFrame.reset_index()
    countFrame['index'] = countFrame['index'].map(lambda x : int(x)+1)
    countFrame['level_0'] = countFrame['level_0'].map(lambda x : int(x)+1)



    matches.columns = ['Event','Date','Venue','Liked Artists']
    matches.Date = matches.Date.map(lambda x: x.strftime('%m/%d/%Y') if pd.notnull(x) else '')
    matches = matches.reset_index()
    matches['index'] = matches['index'].map(lambda x : int(x)+1)

    # matches.style.set_properties(subset=['Date'], **{'width': '100px'})

    return [matches, countFrame]
