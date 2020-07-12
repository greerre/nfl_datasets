import requests
from datetime import datetime
from bs4 import BeautifulSoup
import time
import random
import pandas as pd
import numpy

## set initial params for link scraping ##
season_start = 2009
season = season_start
season_week = 1
data_path = 'YOUT FILE PATH'

## set a max scrape date based on current date ##
if int(datetime.today().month) < 6:
    max_season = int(datetime.today().year) - 1
else:
    max_season = int(datetime.today().year)

## go week by week scraping all the link with "line-movement" in the url ##
line_movement_links = []
line_movement_df_data = []
while season <= max_season:
    missing_week_count = 0
    while season_week <= 22:
        time.sleep((2.5 + random.random() * 5))
        print('          Pulling Week {0}, {1}...'.format(season_week,season))
        url = 'http://www.vegasinsider.com/nfl/matchups/matchups.cfm/week/{0}/season/{1}'.format(season_week,season)
        try:
            raw = requests.get(url)
        except:
            print('               Couldnt get URL. Pausing for 10 seconds and retrying... ')
            time.sleep(10)
            try:
                raw = requests.get(url)
            except:
                print('               Couldnt get URL again...')
                print('               Skipping Week...')
                season_week += 1
        parsed = BeautifulSoup(raw.content, "html5lib")
        game_list = parsed.find_all('tr', {'class' : 'viFooter'})
        if len(game_list) == 0:
            print('               No games found. Skipping season...')
            season += 1
            season_week = 23
        for game in game_list:
            try:
                potential_line_move_links = game.find_all('a')
                for link in potential_line_move_links:
                    try:
                        line_move_link = link.get('href')
                        if 'line-movement' in line_move_link:
                            line_movement_links.append('http://www.vegasinsider.com{0}'.format(line_move_link))
                            line_movement_df_data.append({
                                'season' : season,
                                'week' : season_week,
                                'link' : 'http://www.vegasinsider.com{0}'.format(line_move_link),
                            })
                    except:
                        pass
            except:
                print('error on game')
        season_week += 1
    season_week = 1
    print('links found {0}'.format(len(line_movement_links)))
    season += 1

## save links so you don't have to scrape again in the future ##
pd.DataFrame(line_movement_df_data).to_csv('{0}/line_movement_urls.csv'.format(data_path))

## for each link, scrape the line movements ##
line_movement_data = []
for url in line_movement_links:
    ## save current data in case the scraper fails ##
    pd.DataFrame(line_movement_data).to_csv('{0}/line_movements.csv'.format(data_path))
    ## parse some game info from the url ##
    game_date_list = url.split('/time')[0].split('/')[-1].split('-')
    game_month = int(game_date_list[0])
    game_year = int(game_date_list[2])
    teams = url.split('.cfm')[0].split('/')[-1].split('-@-')
    home_team = teams[1]
    away_team = teams[0]
    print(game_year, teams) ## can remove, just lets you know where the script is at ##
    ## be kind, you're scraping! ##
    time.sleep((1.5 + random.random() * 2))
    ## try to load the html in BS4 ##
    try:
        raw = requests.get(url)
        parsed = BeautifulSoup(raw.content, "html5lib")
    except:
        print('               Couldnt get URL. Pausing for 10 seconds and retrying... ')
        time.sleep(10)
        try:
            raw = requests.get(url)
            parsed = BeautifulSoup(raw.content, "html5lib")
        except:
            parsed = None
            continue
    try:
        ## find all the tables that have line movement info, seperated by book ##
        book_tables = parsed.find_all('td', {'class' : 'rt_railbox_border'})
        ## loop through books to scrape line movements ##
        for book in book_tables:
            book_name = book.find_all('tr', {'class' : 'component_head'})[0].text.strip().split(' LINE MOVEMENTS')[0]
            print(book_name) ## can remove ##
            ## find the table with the actual movements ##
            sub_table = book.find_all('table', {'class' : 'rt_railbox_border2'})[0]
            data_rows = sub_table.find_all('tr', {'class' : None})
            for data_row in data_rows:
                cells = data_row.find_all('td')
                if cells[10].text.strip() == '':
                    ## turn date info in first two cells into a timestamp ##
                    partial_date = cells[0].text.split('/')
                    if int(partial_date[0]) == game_month:
                        partial_date_year = game_year + 2000
                    elif int(partial_date[0]) == 12:
                        partial_date_year = game_year + 2000 - 1
                    else:
                        partial_date_year = game_year + 2000
                    partial_date_time = cells[1].text
                    line_time = '{0}/{1}/{2} {3}'.format(
                        partial_date[0],
                        partial_date[1],
                        partial_date_year,
                        partial_date_time
                    )
                    line_date = datetime.strptime(line_time, '%m/%d/%Y %I:%M%p')
                    ## create an epoch for easier future analysis ##
                    line_timestamp = line_date.timestamp()
                    ## parse the favorite and dog text into actual moneylines ##
                    fav_raw = cells[2].text.strip()
                    if '+' in fav_raw:
                        fav = fav_raw.split('+')[0].strip()
                        fav_line = int(fav_raw.split('+')[1].strip())
                    elif '-' in fav_raw:
                        fav = fav_raw.split('-')[0].strip()
                        fav_line = -1 * int(fav_raw.split('-')[1].strip())
                    else:
                        pass
                    dog_raw = cells[3].text.strip()
                    if '+' in dog_raw:
                        dog = dog_raw.split('+')[0].strip()
                        dog_line = int(dog_raw.split('+')[1].strip())
                    elif '-' in dog_raw:
                        dog = dog_raw.split('-')[0].strip()
                        dog_line = -1 * int(dog_raw.split('-')[1].strip())
                    else:
                        pass
                else:
                    pass
                ## output to the data frame ##
                line_movement_data.append({
                    'game_id' : line_movement_links.index(url),
                    'url' : url,
                    'book' : book_name,
                    'line_date' : line_date,
                    'line_timestamp' : line_timestamp,
                    'home_team' : home_team,
                    'away_team' : away_team,
                    'favorite' : fav,
                    'favorite_line' : fav_line,
                    'underdog' : dog,
                    'underdog_line' : dog_line,
                })
    except:
        pass



unformatted_update_df = None
data_rows = []
