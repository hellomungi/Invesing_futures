## https://buyandpray.tistory.com/38 를 참고하여 작성 되었습니다.
## curr_id, smlId 부분 에러를 수정 하였습니다.
## Dataframe 형식으로 데이터를 받도록 수정 했습니다
## DB 저장을 추가 하였습니다.
## 자잘한 에러를 수정하였습니다.


import pandas as pd
import requests, re, time

from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from datetime import date, datetime, timedelta




## 시작날짜와 종료날짜 문자열로 생성
now = datetime.now()
now_time = date.today()
now_day = now.strftime('%Y%m%d')
start_date = (now_time + timedelta(days=-365)).strftime('%Y/%m/%d')
end_date = now.strftime('%Y/%m/%d')


def investing(url):
    ## HistoricalDataAjax 요청하기 위한 headers 작성 
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    baseURL = r"https://kr.investing.com"

    ## 원자재를 뽑아 리스트생성
    commodities = []

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    commodityDiv = soup.find('div', {'id': 'cross_rates_container'})

    for a_tag in commodityDiv.find('tbody').find_all('a'):
        ## 원자재 url <href> 태그에서 추출 
        href = a_tag.get("href") 
        ## 원자재명 추출
        title = a_tag.get("title")

        if "commodities" in href:
            commodities.append((baseURL + href, title))  # url, title 튜플로 저장
    
    
    ids = []

    for URL, _ in commodities:
        historicalURL = URL + "-historical-data"
        
        ## 작성한 headers와 currid, smlId를 ids에 투플로 저장
        response = requests.get(historicalURL, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')

        for script in soup.findAll('script'):
            if script.string and "window.histDataExcessInfo" in script.string:
                histData = script.string.strip().replace("\n", "").replace(" ", "")
                curr_id = re.findall("\d+", histData)
                curr_id = curr_id[0]
                smlId = curr_id[1]
                ids.append((curr_id, smlId))  # 투플로 저장

        time.sleep(0.2)

    
    
    
    formData = {
        "curr_id": "",
        "smlID": "",
        "header": "",
        "st_date": str(start_date),
        "end_date": str(end_date),
        "interval_sec": "Daily",
        "sort_col": "date",
        "sort_ord": "DESC",
        "action": "historical_data"
    }


    POSTURL = r"https://kr.investing.com/instruments/HistoricalDataAjax"
    save_num = 0
    
    for (_, title), (curr_id, smlID) in zip(commodities, ids):
        formData["curr_id"] = curr_id
        formData["smlID"] = smlID
        formData["header"] = title + " 내역"
        
        ## ids와 currid, dmlId 등 formData를 만들어 데이터 호출
        response = requests.post(POSTURL, headers=headers, data=formData)
        response = BeautifulSoup(response.text, "lxml")
        rep_table = response.find('table', {'class': 'genTbl closedTbl historicalTbl'})
        rep_tbody = rep_table.find('tbody')
        rep_tr = rep_tbody.find_all('tr')

        table_df = pd.DataFrame()
        table_num = 0

        for tr in rep_tr:
            ## response data를  dataframe에 저장 
            td = tr.find_all('td')
            table_df.loc[table_num, '날짜'] = pd.to_datetime(td[0].text.replace("년 ", "/").replace("월 ", "/").replace("일", ""))
            table_df.loc[table_num, '종가'] = td[1].text
            table_df.loc[table_num, '시가'] = td[2].text
            table_df.loc[table_num, '고가'] = td[3].text
            table_df.loc[table_num, '저가'] = td[4].text
            table_df.loc[table_num, '거래량'] = td[5].text
            table_df.loc[table_num, '변동'] = td[6].text
            table_num = table_num + 1

        print(table_df)
        save_name = title.replace(" ", "").replace("선물", "")
        table_df['종목명'] = save_name
        table_df['currid'] = str(curr_id)

        if save_num == 0:
            engine = create_engine("mysql+pymysql://root:" + "PASSWORD" + "@localhost:3306/DBNAME?charset=utf8", encoding='utf-8')
            table_df.to_sql(name='TABLENAME', con=engine, if_exists='replace', index=False)
        elif save_num > 0:
            engine = create_engine("mysql+pymysql://root:" + "PASSWORD" + "@localhost:3306/DBNAME?charset=utf8", encoding='utf-8')
            table_df.to_sql(name='TABLENAME', con=engine, if_exists='append', index=False)

        save_num = save_num + 1

        time.sleep(0.2)

if __name__ == '__main__':
    url = r"https://kr.investing.com/commodities/real-time-futures"
    investing(url)
