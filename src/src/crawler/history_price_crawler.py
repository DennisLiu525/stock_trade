from base64 import decode
import pandas
import requests
import sqlite3
from bs4 import BeautifulSoup
import datetime
import io
import os
import threading
import time
import concurrent.futures
import multiprocessing
from sympy import Add


def stock_list():
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
    res = requests.get(url)
    df = pandas.read_html(res.text)[0]

    line1 = df.iloc[:, 0].to_list()

    for index, i in enumerate(line1):
        if((i == '股票')):
            start = index
        elif(i == '上市認購(售)權證'):
            line1 = line1[start+1:index]
            # line1 = [x.split('') for x in line1]
            break

    ur2 = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"
    res = requests.get(ur2)
    df = pandas.read_html(res.text)[0]

    line2 = df.iloc[:, 0].to_list()

    for index, i in enumerate(line2):
        if((i == '股票')):
            start = index
        elif(i == '特別股'):
            line2 = line2[start+1:index]
            break
    id = []
    name = []
    for i in line1:
        group = i.split('\u3000')
        if(len(group) == 2):
            id.append(group[0]+".TW")
            name.append(group[1])
    for i in line2:
        group = i.split('\u3000')
        if(len(group) == 2):
            id.append(group[0]+".TWO")
            name.append(group[1])
    return pandas.DataFrame({"id": id, "name": name})


def craw_tws_history(*args, **kwargs):
    id = kwargs["id"]
    period1 = kwargs["start"]
    period2 = kwargs["end"]
    head = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.66 Safari/537.36 Edg/103.0.1264.44"}
    url = "https://query1.finance.yahoo.com/v7/finance/download/"+id+"?period1=" +\
        period1+"&period2="+period2+"&interval=1d&events=history&includeAdjustedClose=true"
    res = requests.get(url, headers=head).content
    df = pandas.read_csv(io.StringIO(res.decode('utf-8')))
    return df


def AddToSqlite(lock, id: str, df: pandas.DataFrame, *args, **kwargs):
    df = kwargs["df"]
    id = kwargs["id"]
    dbname = os.getcwd() + '\\resource\\HistoryData.db'
    db = sqlite3.connect(dbname)
    with lock:
        df.to_sql(con=db, name=id, if_exists='replace', index=False)


def runthread(id: str, start: str, end: str, lock: multiprocessing.Lock):
    df = craw_tws_history(start=str(int(start)),
                          end=str(int(end)), id=id)
    AddToSqlite(lock, id=id, df=df)
    print("update " + id)


def craw_all(id: str, start: str, end: str, lock: multiprocessing.Lock):
    stack = stock_list()
    m = multiprocessing.Manager()
    lock = m.Lock()
    i = 0
    while(i < len(stack)):
        if i == len(stack):
            break
        id = stack["id"][i]
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.submit(runthread, id, period1, now_sec, lock)
        i += 1


def crawl(id: str, start: str, end: str):
    m = multiprocessing.Manager()
    lock = m.Lock()
    runthread(id, period1, now_sec, lock)


if __name__ == "__main__":
    now_sec = datetime.datetime.now(tz=None).timestamp()
    period1 = now_sec - datetime.timedelta(days=1800).total_seconds()
    crawl("00663.TW", period1, now_sec)
