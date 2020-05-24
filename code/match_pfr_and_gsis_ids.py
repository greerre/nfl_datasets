import pandas as pd
import numpy

## file locations ##
nfl_roster = 'https://raw.githubusercontent.com/guga31bb/nflfastR-data/master/roster-data/roster.csv'
meta_file = 'https://raw.githubusercontent.com/greerre/nfl_datasets/master/data/player_meta.csv'
output_folder = 'YOUR OUTPUT LOCATION'

## load data ##
nfl_roster_df = pd.read_csv(nfl_roster)
player_meta_df = pd.read_csv(meta_file)


## filter and prep for joins ##
## gsis is what joins to pbp, remove if missing ##
nfl_roster_df = nfl_roster_df.dropna(subset=['teamPlayers.gsisId'])

## NFL FORMATTING ##
## make date matchable to pfr ##
nfl_roster_df['dob'] = (
    nfl_roster_df['teamPlayers.birthDate'].str[6:] + '-' +
    nfl_roster_df['teamPlayers.birthDate'].str[:2] + '-' +
    nfl_roster_df['teamPlayers.birthDate'].str[3:5]
)

## filter and rename ##
nfl_roster_df = nfl_roster_df[[
    'teamPlayers.nflId',
    'teamPlayers.esbId',
    'teamPlayers.gsisId',
    'teamPlayers.displayName',
    'teamPlayers.lastName',
    'dob'
]].drop_duplicates().rename(columns={
    'teamPlayers.nflId' : 'nfl_id',
    'teamPlayers.esbId' : 'esb_id',
    'teamPlayers.gsisId' : 'gsis_id',
    'teamPlayers.displayName' : 'full_name',
    'teamPlayers.lastName' : 'last_name',
})


## PFR FORMATING ##
## remove Jrs and III's from pfr ##
player_meta_df['last_name'] = player_meta_df['last_name'].str.replace(' Jr.', '')
player_meta_df['last_name'] = player_meta_df['last_name'].str.replace(' Sr.', '')
player_meta_df['last_name'] = player_meta_df['last_name'].str.replace(' IV', '')
player_meta_df['last_name'] = player_meta_df['last_name'].str.replace(' III', '')
player_meta_df['last_name'] = player_meta_df['last_name'].str.replace(' II', '')

## my lazy scraper leaves unnamed columns...remove...##
player_meta_columns = player_meta_df.columns.to_list()

drop_columns = []
for col in player_meta_columns:
    if 'Unnamed' in col:
        drop_columns.append(col)
    else:
        pass

player_meta_df = player_meta_df.drop(columns=drop_columns)


## JOIN LOGIC ##
## Join logic could be made much easier to read and unpack w/ a function, but ##
## kept everything vectorized for speed...this seems uncessary and may change in ##
## a futuer version ##

## join on last name and dob ##
## note that this will duplicate players w/ same last name and dob ##
nfl_joined_df = pd.merge(
    nfl_roster_df,
    player_meta_df,
    on=['last_name', 'dob'],
    how='left'
)


## some DOBs are off by a day, year, month, etc ##
## fill in missing records with imprecise matches ##

## first create a df with just the missing to avoid creating mass duplicates ##
nfl_joined_missing_df = nfl_joined_df.copy()
nfl_joined_missing_df = nfl_joined_missing_df[nfl_joined_missing_df['pfr_id'].isna()]

## get a list of already matched IDs to avoid double matching similar dob name combos ##
matched_pfr_ids = nfl_joined_df['pfr_id'].to_list()


## try year and month ##
nfl_joined_missing_df['dob_yyyymm'] = (
    nfl_joined_missing_df['dob'].str[:4] +
    nfl_joined_missing_df['dob'].str[5:7]
)

meta_fuzzy_df = player_meta_df.copy()
meta_fuzzy_df = meta_fuzzy_df[~numpy.isin(meta_fuzzy_df['pfr_id'], matched_pfr_ids)]
meta_fuzzy_df['dob_yyyymm'] = (
    meta_fuzzy_df['dob'].str[:4] +
    meta_fuzzy_df['dob'].str[5:7]
)

## only match pfr id to keep columns clean. Can match rest after ##
nfl_joined_missing_df = pd.merge(
    nfl_joined_missing_df,
    meta_fuzzy_df[['pfr_id','last_name', 'dob_yyyymm']].rename(columns={
        'pfr_id' : 'pfr_id_yyyymm',
    }),
    on=['last_name', 'dob_yyyymm'],
    how='left'
)

## try year and day ##
nfl_joined_missing_df['dob_yyyydd'] = (
    nfl_joined_missing_df['dob'].str[:4] +
    nfl_joined_missing_df['dob'].str[8:10]
)

meta_fuzzy_df = player_meta_df.copy()
meta_fuzzy_df = meta_fuzzy_df[~numpy.isin(meta_fuzzy_df['pfr_id'], matched_pfr_ids)]
meta_fuzzy_df['dob_yyyydd'] = (
    meta_fuzzy_df['dob'].str[:4] +
    meta_fuzzy_df['dob'].str[8:10]
)

## only match pfr id to keep columns clean. Can match rest after ##
nfl_joined_missing_df = pd.merge(
    nfl_joined_missing_df,
    meta_fuzzy_df[['pfr_id','last_name', 'dob_yyyydd']].rename(columns={
        'pfr_id' : 'pfr_id_yyyydd',
    }),
    on=['last_name', 'dob_yyyydd'],
    how='left'
)

## try month and day ##
nfl_joined_missing_df['dob_mmdd'] = (
    nfl_joined_missing_df['dob'].str[5:7] +
    nfl_joined_missing_df['dob'].str[8:10]
)

meta_fuzzy_df = player_meta_df.copy()
meta_fuzzy_df = meta_fuzzy_df[~numpy.isin(meta_fuzzy_df['pfr_id'], matched_pfr_ids)]
meta_fuzzy_df['dob_mmdd'] = (
    meta_fuzzy_df['dob'].str[5:7] +
    meta_fuzzy_df['dob'].str[8:10]
)

## only match pfr id to keep columns clean. Can match rest after ##
nfl_joined_missing_df = pd.merge(
    nfl_joined_missing_df,
    meta_fuzzy_df[['pfr_id','last_name', 'dob_mmdd']].rename(columns={
        'pfr_id' : 'pfr_id_mmdd',
    }),
    on=['last_name', 'dob_mmdd'],
    how='left'
)

## combine fuzzy match columns into single match ##
nfl_joined_missing_df['pfr_id'] = nfl_joined_missing_df['pfr_id'].combine_first(
    nfl_joined_missing_df['pfr_id_yyyymm']
).combine_first(
    nfl_joined_missing_df['pfr_id_yyyydd']
).combine_first(
    nfl_joined_missing_df['pfr_id_mmdd']
)

## create pairings ##
nfl_joined_missing_pairs_df = nfl_joined_missing_df[['gsis_id','pfr_id']].drop_duplicates()

## re add pfr data and rename for join on main file ##
nfl_joined_missing_pairs_df = pd.merge(
    nfl_joined_missing_pairs_df,
    player_meta_df[[
        'pfr_id',
        'first_name',
        'height',
        'weight',
        'college',
        'draft_year',
        'draft_position',
        'combine_forty',
        'combine_bench_reps',
        'combine_broad_jump',
        'combine_shuttle',
        'combine_cone',
        'combine_vertical'
    ]].rename(columns={
        'first_name' : 'first_name_fuzzy',
        'height' : 'height_fuzzy',
        'weight' : 'weight_fuzzy',
        'college' : 'college_fuzzy',
        'draft_year' : 'draft_year_fuzzy',
        'draft_position' : 'draft_position_fuzzy',
        'combine_forty' : 'combine_forty_fuzzy',
        'combine_bench_reps' : 'combine_bench_reps_fuzzy',
        'combine_broad_jump' : 'combine_broad_jump_fuzzy',
        'combine_shuttle' : 'combine_shuttle_fuzzy',
        'combine_cone' : 'combine_cone_fuzzy',
        'combine_vertical' : 'combine_vertical_fuzzy',
    }),
    on=['pfr_id'],
    how='inner'
).rename(columns={
    'pfr_id' : 'pfr_id_fuzzy',
})

## merge back to main df ##
nfl_joined_df = pd.merge(
    nfl_joined_df,
    nfl_joined_missing_pairs_df,
    on=['gsis_id'],
    how='left'
)

## coalesce fuzzy matches ##
nfl_joined_df['pfr_id'] = nfl_joined_df['pfr_id'].combine_first(nfl_joined_df['pfr_id_fuzzy'])
nfl_joined_df['first_name'] = nfl_joined_df['first_name'].combine_first(nfl_joined_df['first_name_fuzzy'])
nfl_joined_df['height'] = nfl_joined_df['height'].combine_first(nfl_joined_df['height_fuzzy'])
nfl_joined_df['weight'] = nfl_joined_df['weight'].combine_first(nfl_joined_df['weight_fuzzy'])
nfl_joined_df['college'] = nfl_joined_df['college'].combine_first(nfl_joined_df['college_fuzzy'])
nfl_joined_df['draft_year'] = nfl_joined_df['draft_year'].combine_first(nfl_joined_df['draft_year_fuzzy'])
nfl_joined_df['draft_position'] = nfl_joined_df['draft_position'].combine_first(nfl_joined_df['draft_position_fuzzy'])
nfl_joined_df['combine_forty'] = nfl_joined_df['combine_forty'].combine_first(nfl_joined_df['combine_forty_fuzzy'])
nfl_joined_df['combine_bench_reps'] = nfl_joined_df['combine_bench_reps'].combine_first(nfl_joined_df['combine_bench_reps_fuzzy'])
nfl_joined_df['combine_broad_jump'] = nfl_joined_df['combine_broad_jump'].combine_first(nfl_joined_df['combine_broad_jump_fuzzy'])
nfl_joined_df['combine_shuttle'] = nfl_joined_df['combine_shuttle'].combine_first(nfl_joined_df['combine_shuttle_fuzzy'])
nfl_joined_df['combine_cone'] = nfl_joined_df['combine_cone'].combine_first(nfl_joined_df['combine_cone_fuzzy'])
nfl_joined_df['combine_vertical'] = nfl_joined_df['combine_vertical'].combine_first(nfl_joined_df['combine_vertical_fuzzy'])

## remove fuzzy columns from main data set ##
nfl_joined_df = nfl_joined_df.drop(columns=[
    'pfr_id_fuzzy',
    'first_name_fuzzy',
    'height_fuzzy',
    'weight_fuzzy',
    'college_fuzzy',
    'draft_year_fuzzy',
    'draft_position_fuzzy',
    'combine_forty_fuzzy',
    'combine_bench_reps_fuzzy',
    'combine_broad_jump_fuzzy',
    'combine_shuttle_fuzzy',
    'combine_cone_fuzzy',
    'combine_vertical_fuzzy'
])



## deal with duplicates ##
## duplicated gsis_ids ##
nfl_joined_df_duplicates = nfl_joined_df[['gsis_id']]
gsis_dupes = nfl_joined_df_duplicates[nfl_joined_df_duplicates.duplicated(keep='first')]['gsis_id'].to_list()

## mark dupes in data set ##
nfl_joined_df['dupe'] = numpy.where(numpy.isin(nfl_joined_df['gsis_id'], gsis_dupes), 1, 0)

## use first name to remove incorrect matches ##
## Note, first name matching could introduce more fragility in the initial match ##
## which is why it's only used as necessary to dedupe ##
def first_in_full(row):
    if row['first_name'] is numpy.nan:
        ## if there was no pfr match, skip ##
        row['dupe'] = row['dupe']
    elif row['first_name'] in row['full_name'].split(row['last_name'])[0]:
        ## if first name is contained in the full name, remove dupe flag ##
        row['dupe'] = 0
    else:
        row['dupe'] = row['dupe']
    return row


nfl_joined_df = nfl_joined_df.apply(first_in_full, axis=1)
## note this doesn't work for buddy howell, for whom pfr has a bug that results in two entries ##
## drop dupes ##
nfl_joined_df = nfl_joined_df[nfl_joined_df['dupe'] == 0]
nfl_joined_df = nfl_joined_df.drop(columns=[
    'dupe'
])

## dupes will also go the other way (two gsis_ids to one pfr_id ##
## find and erase these matches ##
pfr_df_duplicates = nfl_joined_df[['pfr_id']]
pfr_dupes = pfr_df_duplicates[pfr_df_duplicates.duplicated(keep='first')]['pfr_id'].to_list()

## mark dupes in data set ##
nfl_joined_df['dupe'] = numpy.where(numpy.isin(nfl_joined_df['pfr_id'], pfr_dupes), 1, 0)

## use first time to determine if tha match is accurate ##
## b/c we want to preserve gsis, scrub out the pfr data, don't drop row ##
def pfr_dupe_scrub(row):
    if row['first_name'] is numpy.nan:
        ## if there was no pfr match, skip ##
        pass
    elif row['dupe'] == 1:
        if row['first_name'] in row['full_name'].split(row['last_name'])[0]:
            ## if first name is contained in the full name, leave pfr data ##
            pass
        else:
            ## else scrub bad matches, leaving row w/ nfl roster data ##
            row['pfr_id'] = numpy.nan
            row['first_name'] = numpy.nan
            row['height'] = numpy.nan
            row['weight'] = numpy.nan
            row['college'] = numpy.nan
            row['draft_year'] = numpy.nan
            row['draft_position'] = numpy.nan
            row['combine_forty'] = numpy.nan
            row['combine_bench_reps'] = numpy.nan
            row['combine_broad_jump'] = numpy.nan
            row['combine_shuttle'] = numpy.nan
            row['combine_cone'] = numpy.nan
            row['combine_vertical'] = numpy.nan
    else:
        pass
    return row


nfl_joined_df = nfl_joined_df.apply(pfr_dupe_scrub, axis=1)

nfl_joined_df = nfl_joined_df.drop(columns=[
    'dupe'
])

nfl_joined_reverse_df = pd.merge(
    player_meta_df,
    nfl_roster_df,
    on=['last_name', 'dob'],
    how='left'
)

## export roster file ##
nfl_joined_df.to_csv('{0}/roster_w_pfr.csv'.format(output_folder))


## reverse join to the pfr file ##
gsis_to_pfr_df = nfl_joined_df[['gsis_id', 'pfr_id']]

pfr_joined_df = pd.merge(
    player_meta_df.copy(),
    gsis_to_pfr_df,
    on=['pfr_id'],
    how='left'
).drop_duplicates()

pfr_joined_df.to_csv('{0}/pfr_w_roster.csv'.format(output_folder))
dupe_df = pd.merge(
    pfr_joined_df[pfr_joined_df['pfr_id'].duplicated(keep=False)],
    nfl_roster_df,
    on=['gsis_id'],
    how='left'
)
