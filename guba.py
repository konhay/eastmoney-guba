import pandas as pd
import numpy as np
import datetime
import re
import socket
import jieba
import sqlalchemy
import codecs
from urllib.request import build_opener
from urllib.request import install_opener
from urllib.request import urlopen
from aip import AipNlp
from bs4 import BeautifulSoup
from sqlalchemy.exc import *
from .settings import STOCK_NAME_STRING


def getNumofCommonSubstr(str1, str2):
    '''
    decription: 返回str1与str2的最长公共子串及其长度
    '''
    len1 = len(str1)
    len2 = len(str2)
    record = [[0 for i in range(len2 + 1)] for j in range(len1 + 1)]  # 多一位
    maxNum = 0  # 最长匹配长度
    p = 0  # 匹配的起始位
    for i in range(len1):
        for j in range(len2):
            if str1[i] == str2[j]:
                # 相同则累加
                record[i + 1][j + 1] = record[i][j] + 1
                if record[i + 1][j + 1] > maxNum:
                    # 获取最大匹配长度
                    maxNum = record[i + 1][j + 1]
                    # 记录最大匹配长度的终止位置
                    p = i + 1
    maxSub = str1[p - maxNum:p] #最长公共子串
    return maxSub, maxNum


def get_page_comment(pageUrl, year):
    '''
    description: 获取单个页面的页面评论,并按标题内容过滤,按发帖时间排序
    pageUrl: 例如 http://guba.eastmoney.com/list,zssh000001,f_1.html
    year: int
    '''
    html = urlopen(pageUrl)
    bsObj = BeautifulSoup(html, 'html.parser')
    fj7 = bsObj.find("div", {"id": "articlelistnew"}).findAll("div", {"class": "normal_post"})
    html.close()
    comment_list = []
    for j in fj7:
        # get comment datetime
        time = str(year) + '-' + j.find("span", {"class": "l5 a5"}).get_text()
        dtime = datetime.datetime.strptime(time, '%Y-%m-%d %H:%M')

        # get comment title and author
        title = j.find("span", {"class": "l3 a3"}).find('a').get('title')
        if title is None :continue;
        author = j.find("span", {"class": "l4 a4"}).find('font').get_text()

        # 检验标题是否含有特殊字符
        pattern = re.compile(r'[【】$]')
        special_symbol = pattern.findall(str(title))

        # 检验标题是否包含个股名称
        # 奥福环保天奈科技合康新能兆新股份旷达科技海陆重工新能泰山合锻智能铜峰...
        maxSub, maxNum = getNumofCommonSubstr(title, STOCK_NAME_STRING)

        if len(special_symbol) == 0 and maxNum < 4 and author != "股吧" and author[-2:] !='资讯':
            comment_list.append([dtime, title, author])
            print(str(dtime), title, ":", author)
    return comment_list


def get_batch_comment(start_index=1, count=1, end_date=None):
    '''
    description: 批量获取上证股吧评论，不支持跨年调用
    start_index: 起始页码
    count: 抓取页数，与end_date二选一
    end_date: 结束日期(含)，与count二选一
    用法1: guba(start_index=1, count=10) 仅支持今年
    用法2: guba(start_index=100, end_date='2021-12-31') 仅支持今年
    '''
    baseUrl = 'http://guba.eastmoney.com/list,zssh000001,f_'
    comment_batch = []

    if end_date is not None :
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
        end_dtime = datetime.datetime.combine(end_date, datetime.time(13, 10)) #只抓取stop_date下午1点10分之后的数据
        while(True) :
            pageUrl = baseUrl + str(start_index) + '.html'
            print("pageUrl:", pageUrl)
            comment_list = get_page_comment(pageUrl, end_date.year)
            if np.array(comment_list).T[0].min() > end_dtime :
                comment_batch = comment_batch + comment_list
                start_index = start_index + 1
            else :
                break
    else :
        for i in range(count):
            pageUrl = baseUrl + str(start_index+i) + '.html'
            print("pageUrl:", pageUrl)
            comment_list = get_page_comment(pageUrl, datetime.date.today().year)
            comment_batch = comment_batch + comment_list

    print(len(comment_batch), "comments found totally.")
    return comment_batch


def insert_comment(comment_list, code='zssh000001'):
    '''
    description: 将上证股吧评论插入mysql
    comment_list: get from get_page_comment or get_batch_comment
    code: zssh000001(上证), 600000(浦发银行), ...
    '''
    if len(comment_list) != 0:
        df = pd.DataFrame(comment_list, columns=['dtime','title','author'])
        df.drop_duplicates(inplace=True)
        df['code'] = code
        try:
            engine = sqlalchemy.create_engine("mysql://root:root@localhost:3306/finance?charset=utf8")
            df.to_sql('east_guba_cmt', engine, if_exists='append', index=False)
            engine.dispose()
            print(len(df), 'rows inserted.')
        except IntegrityError as e :
            # 能确保报错之前的新记录都被插入
            print('insert failure')
    else :
        print('empty comment_list')

    # todo: baseUrl = "http://guba.eastmoney.com/list," + code + ",f_"


def update_comment(code="zssh000001", index=1, year=None):
    '''
    description: 从给定位置向前插入全部的评论
    code: 默认为上证指数
    index: 默认为1
    year: 默认为当前年份,如果想自定义年份，请确保给出一个合适的index值
    '''
    baseUrl = "http://guba.eastmoney.com/list," + code + ",f_"
    dt = datetime.datetime.now().date()
    tm = datetime.datetime.now().time()
    if year is None:
        year = dt.year
    start_time = datetime.time(9, 0)
    end_time = datetime.time(17, 0)

    if tm < end_time and tm > start_time:
        return 0

    end_flag = False
    comments = []
    while (not end_flag):
        pageUrl = baseUrl + str(index) + '.html'
        print("pageUrl:", pageUrl)
        cmt_list = get_page_comment(pageUrl, year)
        for i in cmt_list:
            if i[0].date() == dt:
                if i[0].time() < end_time and i[0].time() > start_time:
                    comments.append(i)
            else:
                if len(comments) != 0 :
                    df = pd.DataFrame(comments, columns=["dtime","title","author"])
                    df = df.drop_duplicates()
                    df["code"]=code
                    try:
                        engine = sqlalchemy.create_engine("mysql://root:root@localhost:3306/finance?charset=utf8")
                        df.to_sql('east_guba_cmt', engine, if_exists='append', index=False)
                        print(dt, "insert data success:", len(df))
                        comments = []
                    except IntegrityError:
                        # 能确保报错之前的新记录都被插入
                        print(dt, "insert skip")
                        end_flag = True
                        break
                else :
                    print(dt, "len(comments) is zero.")
                dt = dt - datetime.timedelta(days=1)
                if(dt.year!=year):
                    print("dt is", dt)
                    return 0
        index = index +1
    return 1


def get_segment_list(comment_list):
    '''
    description: 结巴分词
    comment_list: get from get_page_comment or get_batch_comment
    '''
    segment_list = []
    for comment in comment_list:
        # ss = SnowNLP(comment[1])
        # print(round(ss.sentiments,3), comment[1])
        segments = jieba.cut(comment[1], cut_all=False)

        def remove_stopwords(segments):
            '''
            description: 过滤停止词
            segments: 剔除停止词前的结巴分词
            segments_nstop: 剔除停止词后的结巴分词
            '''
            stopwords = list([line.strip() for line in open('stopwords.txt')])
            stopwords.append("XXX")
            stopwords.append("YYY")
            segments_nstop = []
            for segment in segments:
                if segment[0] not in stopwords:
                    segments_nstop.append(segment)
            return segments_nstop

        segments = remove_stopwords(segments)
        print(" ".join(segments))
        segment_list.append(segments)
    return segment_list


def label_comment(comment_list):
    '''
    description: 给评论内容打标签,即看多还是看空
    comment_list: get from get_page_comment or get_batch_comment
    txt: words_put and words_call needs to be constantly expanded
    '''
    put_words = list([line.strip() for line in open('words_put.txt', encoding='UTF-8')])
    call_words = list([line.strip() for line in open('words_call.txt', encoding='UTF-8')])
    put_label = False
    call_label = False
    i = 1
    for comment in comment_list:
        print('正在处理第{}条,还剩{}条'.format(i, len(comment_list)-i))
        i = i+1
        for word in put_words:
            if word in comment[1] :
                put_label = True
                break
        for word in call_words:
            if word in comment[1] :
                call_label = True
                break
        if put_label and not call_label:
            comment.append('put')
        elif not put_label and call_label:
            comment.append('call')
        else :
            comment.append('unknown')
    return comment_list


def get_sentiment(text):
    '''
    description: 利用百度nlp应用,进行文本情绪分析
    text: chinese string
    reference: https://github.com/Baidu-AIP
    '''
    def initial_client():
        '''
        description: 初始化百度nlp应用
        reference: https://console.bce.baidu.com/
        '''
        APP_ID = '25527453'
        API_KEY = 'W4iGLrhK45Uo6uRt5izx4FjH'
        SECRET_KEY = 'wFbty4qqxB2gpzjeMiPcl89T1PkGZTjb'
        client = AipNlp(APP_ID, API_KEY, SECRET_KEY)
        return client
    try:
        client = initial_client()
        items = client.sentimentClassify(text)['items'][0] #dict
        positive_prob = items['positive_prob']
        confidence = items['confidence']
        negative_prob = items['negative_prob']
        sentiment = items['sentiment'] #0表示消极，1表示中性，2表示积极
        output = '{}\t{}\t{}\n'.format(positive_prob, confidence, sentiment)
        # f = codecs.open('sentiment.txt', 'a+', 'utf-8')
        # f.write(output)
        # f.close()
        print(output)
    except Exception as e:
        print(e)


def get_content():
    data = pd.DataFrame(pd.read_excel('eastmoney.xlsx', sheet_name=0))
    data.columns = ['Dates', 'viewpoints']  # 重设表头
    data = data.sort_values(by=['Dates'])  # 按日期排列
    vdata = data[data.Dates >= startdate]  # 提取对应日期的数据
    newvdata = vdata.groupby('Dates').agg(lambda x: list(x))  # 按日期分组，把同一天的评论并到一起
    return newvdata


# todo: save data in txt


if __name__ == '__main__':

    # 403 Forbidden
    opener = build_opener()
    opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]
    install_opener(opener)
    socket.setdefaulttimeout(20)

    # todo: test functions

    get_batch_comment(count=2)
