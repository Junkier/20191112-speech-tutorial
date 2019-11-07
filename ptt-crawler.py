
# coding: utf-8

# In[ ]:


import requests , time , random , arrow
from pymongo import MongoClient
from bs4 import BeautifulSoup as bs 




client  = MongoClient("localhost",27017)
col     = client["tutorial"]["speech-demo"]
baseUrl = "https://www.ptt.cc"
initUrl = "https://www.ptt.cc/ask/over18"




def getPrevLink(soup):
    link = soup.select("div.btn-group-paging a") [1]["href"]
    return "{}{}".format(baseUrl,link)

def getArticleList(soup):
    return [ 
        {
            "title" : ele.text , 
             "link" : "{}{}".format(baseUrl,ele["href"])
        } for ele in soup.select("div.r-ent div.title a")
    ]

def getContent(soup):
    mainTag = soup.select("div#main-content")[0]
    dirtyTag = mainTag.select("div,span")
    while len(dirtyTag)>0:
        for ele in dirtyTag: ele.extract()
        dirtyTag = mainTag.select("div,span")
    content = mainTag.text.strip()
    return content


def getResp(ele):
    return {
        "user-id" : ele.select("span.push-userid")[0].text.strip(),
        "type"    : ele.select("span.push-tag")[0].text.strip(),
        "content" : ele.select("span.push-content")[0].text.strip().replace(": ",""),
        "time"    : " ".join(ele.select("span.push-ipdatetime")[0].text.strip().split(" ")[1:])
    }


def getPostData(sess,link):
    res3 = sess.get(link,headers=headers)

    soup2 = bs(res3.text,"lxml")

    subData = soup2.select("div#main-content")[0].select("div.article-metaline span.article-meta-value")

    # 標題
    title = subData[0].text

    # 時間
    time = subData[2].text

    # 作者
    author = subData[1].text

    # 回應
    rawResp = soup2.select("div#main-content div.push")
    resp = [getResp(ele) for ele in rawResp]

    # 內容
    content = getContent(soup2)
    
    return {
        "link"    : link,
        "title"   : title,
        "author"  : author,
        "time"    : time,
        "content" : content,
        "resp"    : resp
    }


def commitToMongoDB(dataList):
    col.insert_many(dataList)
    dataList = []




### Main
payload = {
    "from" : "/bbs/Gossiping/index.html",
    "yes"  : "yes"
}

headers = {
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"
}

timeFormat = "YYYY-MM-DD HH:mm:ss"

sess = requests.session()

# 滿18歲登入
res = sess.post(initUrl,data = payload , headers = headers)
soup = bs(res.text,"lxml")

print("[{}][Step1] 滿18歲登入驗證 ok.".format(arrow.now().format(timeFormat)))
print("="*80)


#####
linkEles = []

# 取得文章列表
linkEles += getArticleList(soup)


# 翻回前頁 (ex: 前三頁)
num = 2
for i in range(num):
    previousLink = getPrevLink(soup)
    res = sess.get(previousLink , headers = headers)
    soup = bs(res.text,"lxml")
    linkEles += getArticleList(soup)
    print("[{}] 第{}頁文章列表 is done.".format(arrow.now().format(timeFormat),i+1))

print("[{}][Step2] 取得前{}頁文章列表 ok.".format(arrow.now().format(timeFormat),num))
print("="*80)


#####
# 取得本文標題 , 時間 , 內容 , 回應
dataList = []

for ele in linkEles:
    data = getPostData(sess,ele["link"])
    dataList.append(data)
    print("[{}]{} {} is done.".format(arrow.now().format(timeFormat),ele["title"],ele["link"]))
    
    time.sleep(random.random())
    
    
    if len(dataList)>20 : 
        print("[{}] *** 存入 MongoDB ok. ***".format(arrow.now().format(timeFormat)))
        commitToMongoDB(dataList)
        dataList = []

print("[{}][Step3] 文章抓取 ok.".format(arrow.now().format(timeFormat)))
print("="*80)



#####
if len(dataList)>0 : commitToMongoDB(dataList)
    
print("[{}][Step4] 存入 MongoDB ok.".format(arrow.now().format(timeFormat)))



print("Done.")

