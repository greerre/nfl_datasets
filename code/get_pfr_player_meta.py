import requests
import time
import random
from bs4 import BeautifulSoup
import pandas as pd
import numpy


## file locations ##
output_folder = 'YOUR FOLDER'

## get all unpulled player ids ##
alphabet = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
    'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
    'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
    'Y', 'Z'
]

ids = []

## first get all ids ##
for letter in alphabet:
    time.sleep((.75 + random.random() * .5))
    ## get page w/ all players by letter ##
    raw = requests.get('https://www.pro-football-reference.com/players/{0}/'.format(letter))
    soup = BeautifulSoup(raw.content, 'html.parser')
    player_names = soup.find_all('div', {'class' : 'section_content', 'id' : 'div_players'})[0]
    for player in player_names.find_all('p'):
        ## only players since 1999 ##
        last_year = int(player.text.split('-')[-1])
        if last_year < 1999:
            pass
        else:
            anchor = player.find('a')['href']
            id = anchor.split('.htm')[0].split('/')[-1]
            ids.append(id)


## look for existing data and ids ##
try:
    existing_data_df = pd.read_csv('{0}/player_meta.csv'.format(output_folder))
    existing_ids = existing_data_df['pfr_id'].to_list()
except:
    existing_data_df = pd.DataFrame(columns=[
            'pfr_id', 'last_name', 'first_name', 'height', 'weight','dob',
            'college','draft_year','draft_position','combine_forty', 'combine_bench_reps',
            'combine_broad_jump', 'combine_shuttle', 'combine_cone', 'combine_vertical'
    ])
    existing_ids = []

## take only missing
to_pull_ids = []
for id in ids:
    if id in existing_ids:
        pass
    else:
        to_pull_ids.append(id)



## Player Meta Scraper ##

## define helper functions ##
def find_last_name_abr(player_id):
    ## parse the player id to identify the last name abreviation ##
    ## For players with spaces in names, a simple split will not ##
    ## consistently yield the correct last name ##
    ## The player id will contain lowercase x's when the name is less than 4 ##
    ## chars ##
    for c in range(1,len(player_id)):
        if player_id[c].isupper() or player_id[c] == 'x':
            return player_id[:c]
        else:
            pass
    return player_id[:3]


def convert_height(raw_height):
    ## convert human readable height to inches ##
    try:
        feet = int(raw_height.split('-')[0])
        inches = int(raw_height.split('-')[1])
        return feet * 12 + inches
    except:
        return numpy.nan


def convert_weight(raw_weight):
    ## convert human readable weight to int ##
    try:
        return int(raw_weight.split('lb')[0])
    except:
        return numpy.nan


def convert_college(raw_college):
    ## pull college out of raw text ##
    try:
        return raw_college.find_all('a')[0].text
    except:
        return numpy.nan


def convert_draft_pos(raw_draft):
    ## convert draft text to overall position ##
    try:
        overall_text = raw_draft.split('(')[1].split(')')[0]
        overall_pos = int(overall_text.split('st')[0].split('nd')[0].split('rd')[0].split('th')[0])
        draft_year = int(raw_draft.split('of the ')[1].split(' ')[0])
        return (overall_pos, draft_year)
    except:
        return (numpy.nan, numpy.nan)


def get_combine(soup, field):
    ## try to pull combine stats for a given field ##
    try:
        ## pfr comments mess up bs4 parse, so pull it out manually ##
        combine_info_effed = str(soup.find('div', {'id' : 'all_combine'}))
        combine_fixed = combine_info_effed.split('<!--')[1].split('-->')[0]
        new_parsed = BeautifulSoup(combine_fixed, 'html.parser')
        return float(new_parsed.find_all('td', {'data-stat' : field})[0].text)
    except:
        return numpy.nan


def pull_player_data(player_id):
    url = 'https://www.pro-football-reference.com/players/{0}/{1}.htm'.format(
        player_id[:1],
        player_id
    )
    raw = requests.get(url)
    parsed = BeautifulSoup(raw.content, 'html.parser')
    ## get high level meta div ##
    meta_div = parsed.find_all('div', {'itemtype' : 'https://schema.org/Person'})[0]
    ## get name ##
    player_name = meta_div.find_all('h1', {'itemprop' : 'name'})[0].text
    last_name_abr = find_last_name_abr(player_id)
    first_name = player_name[:(player_name.find(last_name_abr)-1)]
    last_name = player_name[player_name.find(last_name_abr):]
    ## get height ##
    player_height_raw = meta_div.find_all('span', {'itemprop' : 'height'})[0].text
    player_height = convert_height(player_height_raw)
    ## get weight ##
    player_weight_raw = meta_div.find_all('span', {'itemprop' : 'weight'})[0].text
    player_weight = convert_weight(player_weight_raw)
    ## get DOB ##
    player_dob = meta_div.find_all('span', {'itemprop' : 'birthDate'})[0]['data-birth']
    ## get college ##
    college_raw = None
    for i in meta_div.find_all('p'):
        if 'College:' in i.text:
            college_raw = i
        else:
            pass
    college = convert_college(college_raw)
    ## get draft position ##
    draft_pos_raw = None
    for i in meta_div.find_all('p'):
        if 'Draft:' in i.text:
            draft_pos_raw = i.text
        else:
            pass
    draft_pos = convert_draft_pos(draft_pos_raw)[0]
    draft_year = convert_draft_pos(draft_pos_raw)[1]
    ## get combine stats ##
    combine_forty = get_combine(parsed, 'forty_yd')
    combine_bench_reps = get_combine(parsed, 'bench_reps')
    combine_broad_jump = get_combine(parsed, 'broad_jump')
    combine_shuttle = get_combine(parsed, 'shuttle')
    combine_cone = get_combine(parsed, 'cone')
    combine_vertical = get_combine(parsed, 'vertical')
    return({
        'pfr_id' : player_id,
        'last_name' : last_name.strip(),
        'first_name' : first_name.strip(),
        'height' : player_height,
        'weight' : player_weight,
        'dob' : player_dob,
        'college' : college,
        'draft_year' : draft_year,
        'draft_position' : draft_pos,
        'combine_forty' : combine_forty,
        'combine_bench_reps' : combine_bench_reps,
        'combine_broad_jump' : combine_broad_jump,
        'combine_shuttle' : combine_shuttle,
        'combine_cone' : combine_cone,
        'combine_vertical' : combine_vertical,
    })




## Run ##
for id in to_pull_ids:
    print('on id #{0} of {1}'.format(to_pull_ids.index(id)+1, len(to_pull_ids)))
    time.sleep((1 + random.random() * .5))
    ## pull data, append, and save ##
    ## slow, but allows for easy restart if scraper breaks ##
    try:
        existing_data_df = existing_data_df.append(pd.Series(pull_player_data(id)), ignore_index=True)
        existing_data_df.to_csv('{0}/player_meta.csv'.format(output_folder))
    except:
        print('ERROR on {0}'.format(id))
        pass
