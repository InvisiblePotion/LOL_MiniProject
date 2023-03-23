import pymysql
import pandas as pd
import cx_Oracle
import requests
import random
from tqdm import tqdm

dsn = cx_Oracle.makedsn('localhost', 1521, 'xe')
seoul_api_key = '(인증키)'

riot_api_key = '(인증키)'


def db_open():
    global db
    global cursor
    db = cx_Oracle.connect(user='LOL', password='1234', dsn=dsn)
    cursor = db.cursor()
    print('oracle open!')


def oracle_execute(q):
    global db
    global cursor
    try:
        if 'select' in q or 'SELECT' in q:
            df = pd.read_sql(sql=q, con=db)
            return df
        cursor.execute(q)
        return 'oracle 쿼리 성공!'
    except Exception as e:
        print(e)


def oracle_close():
    try:
        global db
        global cursor
        db.commit()
        cursor.close()
        db.close()
        return '오라클 닫힘'
    except Exception as e:
        print(e)


'''
mysql
'''


def connect_mysql(db):
    conn = pymysql.connect(host='localhost', user='root', password='1234', db=db, charset='utf8')
    return conn


def mysql_execute(query, conn):
    cursor_mysql = conn.cursor()
    cursor_mysql.execute(query)
    result = cursor_mysql.fetchall()
    return result


def mysql_execute_dict(query, conn):
    cursor_mysql = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cursor_mysql.execute(query)
    result = cursor_mysql.fetchall()
    return result


def df_creater(url):
    url = url.replace('(인증키)', seoul_api_key).replace('xml', 'json').replace('/5/', '/1000/')
    res = requests.get(url).json()
    key = list(res.keys())[0]
    data = res[key]['row']
    df = pd.DataFrame(data)
    return df


def get_puuid(user):
    url = f'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/{user}?api_key={riot_api_key}'
    res = requests.get(url).json()
    puuid = res['puuid']
    return puuid


def get_match_id(puuid, num):
    url = f'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?type=ranked&start=0&count={num}&api_key={riot_api_key}'
    res = requests.get(url).json()
    return res


def get_matches_timelines(match_id):
    url1 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={riot_api_key}'
    res1 = requests.get(url1).json()
    url2 = f'https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline?api_key={riot_api_key}'
    res2 = requests.get(url2).json()
    return res1, res2


def get_rawdata(tier):
    division_list = ['I', 'II', 'III', 'IV']
    lst = []
    page = random.randrange(1, 100)

    for division in division_list:
        url = f'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{tier}/{division}?page={page}&api_key={riot_api_key}'
        res = requests.get(url).json()
        lst += random.sample(res, 5)

    summornerName_list = list(map(lambda x: x['summonerName'], lst))

    puuid_list = []
    for n in tqdm(summornerName_list):
        try:
            puuid_list.append(get_puuid(n))
        except:
            print(n)
            continue
    match_id_lst = []
    for p in tqdm(puuid_list):
        match_id_lst.extend(get_match_id(p, 2))

    df_create = []
    for match_id in tqdm(match_id_lst):
        matches, timeline = get_matches_timelines(match_id)
        df_create.append([match_id, matches, timeline])

    df = pd.DataFrame(df_create, columns=['match_id', 'matches', 'timeline'])
    return df


def get_match_timeline_df(df):
    df_creater = []
    print('소환사 스텟 생성중...')
    for i in tqdm(range(len(df))):
        try:
            if df.iloc[i]['matches']['info']['gameDuration'] > 900:
                for j in range(10):
                    tmp = []
                    tmp.append(df.iloc[i].match_id)
                    tmp.append(df.iloc[i].matches['info']['gameDuration'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['participantId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['championName'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamPosition'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['teamId'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['win'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['kills'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['deaths'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['assists'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageDealtToChampions'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageTaken'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['challenges']['epicMonsterSteals'])
                    tmp.append(df.iloc[i].matches['info']['participants'][j]['enemyMissingPings'])
                    for k in range(5, 26, 2):
                        try:
                            tmp.append(
                                df.iloc[i].timeline['info']['frames'][k]['participantFrames'][str(j + 1)]['totalGold'])
                        except:
                            tmp.append(0)
                    df_creater.append(tmp)
        except:
            print(i)

    columns = ['matchId', 'gameDuration', 'summonerName', 'participantId', 'championName', 'teamPosition', 'teamId',
               'win', 'kills', 'deaths', 'assists', 'totalDamageDealtToChampions', 'totalDamageTaken',
               'epicMonsterSteals', 'enemyMissingPings', 'g_5', 'g_7', 'g_9', 'g_11', 'g_13', 'g_15', 'g_17', 'g_19',
               'g_21', 'g_23', 'g_25']

    df = pd.DataFrame(df_creater, columns=columns).drop_duplicates()
    print("df 제작 완료")
    return df


def insert_matches_timeline(row):
    query = (
        f"INSERT INTO LOL_MP(matchId, gameDuration, summonerName, participantId, championName, teamPosition, teamId, "
        f"win, kills, deaths,"
        f"assists, totalDamageDealtToChampions,totalDamageTaken, epicMonsterSteals, enemyMissingPings,"
        f"g_5, g_7, g_9, g_11, g_13, g_15, g_17, g_19, g_21, g_23,g_25)"
        f"values ({repr(row.matchId)},{row.gameDuration}, {repr(row.summonerName)}, {row.participantId}, {repr(row.championName)},{repr(row.teamPosition)},"
        f"{row.teamId}, {repr(str(row.win))}, {row.kills}, {row.deaths}, {row.assists}, {row.totalDamageDealtToChampions},{row.totalDamageTaken},"
        f"{row.epicMonsterSteals}, {row.enemyMissingPings}, {row.g_5}, {row.g_7}, {row.g_9}, {row.g_11}, {row.g_13},{row.g_15},{row.g_17}, {row.g_19}, {row.g_21}, {row.g_23}, {row.g_25})"
    )
    oracle_execute(query)
    return
