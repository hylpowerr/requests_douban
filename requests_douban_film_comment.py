import requests,time
from lxml import etree
import time
from all_headers import Headers
from pymongo import MongoClient
import jieba
from collections import Counter
from wordcloud import WordCloud,ImageColorGenerator
from scipy.misc import imread
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

def get_comments(url,headers,start,max_restart_num,movie_name,collection):
    '''
    获取评论
    :param url: 请求页面的url
    :param headers: 请求头
    :param start: 第start条数据开始
    :param max_restart_num: 当获取失败时,最大重新请求次数
    :param movie_name: 电影名字
    :param collection: mongodb数据库的集合
    :return:
    '''
    if start > 4999:
        print("已爬取5000条评论,结束爬取")
        return

    data = {
        'start': start,
        'limit': 20,
        'sort': 'new_score',
        'status': 'P',
    }
    response = requests.get(url=url, headers=headers, params=data)
    tree = etree.HTML(response.text)
    comment_item = tree.xpath('//div[@id ="comments"]/div[@class="comment-item"]')
    len_comments = len(comment_item)
    if len_comments > 0:
        for i in range(1, len_comments + 1):
            votes = tree.xpath('//div[@id ="comments"]/div[@class="comment-item"][{}]//span[@class="votes"]'.format(i))
            commenters = tree.xpath(
                '//div[@id ="comments"]/div[@class="comment-item"][{}]//span[@class="comment-info"]/a'.format(i))
            ratings = tree.xpath(
                '//div[@id ="comments"]/div[@class="comment-item"][{}]//span[@class="comment-info"]/span[contains(@class,"rating")]/@title'.format(
                    i))
            comments_time = tree.xpath(
                '//div[@id ="comments"]/div[@class="comment-item"][{}]//span[@class="comment-info"]/span[@class="comment-time "]'.format(
                    i))
            comments = tree.xpath(
                '//div[@id ="comments"]/div[@class="comment-item"][{}]/div[@class="comment"]/p'.format(i))

            vote = (votes[0].text.strip())
            commenter = (commenters[0].text.strip())
            try:
                rating = (str(ratings[0]))
            except:
                rating = 'null'
            comment_time = (comments_time[0].text.strip())
            comment = (comments[0].text.strip())

            comment_dict = {}
            comment_dict['vote'] = vote
            comment_dict['commenter'] = commenter
            comment_dict['rating'] = rating
            comment_dict['comments_time'] = comment_time
            comment_dict['comments'] = comment

            comment_dict['movie_name'] = movie_name
            #存入数据库

            print("正在存取第{}条数据".format(start+i))
            print(comment_dict)
            collection.update({'commenter': comment_dict['commenter']}, {'$setOnInsert': comment_dict}, upsert=True)


        headers['Referer'] = response.url
        start += 20
        data['start'] = start
        time.sleep(5)
        return get_comments(url, headers, start, max_restart_num,movie_name,collection)
    else:
        # print(response.status_code)
        if max_restart_num>0 :
            if response.status_code != 200:
                print("fail to crawl ,waiting 10s to restart continuing crawl...")
                time.sleep(10)
                headers['User-Agent'] = Headers.getUA()
                print(start)
                return get_comments(url, headers, start, max_restart_num-1, movie_name, collection)
            else:
                print("finished crawling")
                return
        else:
            print("max_restart_num has run out")
            with open('log.txt',"a") as fp:
                fp.write('\n{}--latest start:{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), start))
            return

def get_words_frequency(collection,stop_set):
    '''
    中文分词并返回词频
    :param collection: 数据库的table表
    :param stop_set: 停用词集
    :return:
    '''
    # array = collection.find({"movie_name": "春宵苦短，少女前进吧！ 夜は短し歩けよ乙女","rating":{"$in":['力荐','推荐']}},{"comments":1})
    array = collection.find({"movie_name": "春宵苦短，少女前进吧！ 夜は短し歩けよ乙女","$or":[{'rating':'力荐'},{'rating':'推荐'}]},{"comments":1})
    num = 0
    words_list = []
    for doc in array:
        num+=1
        # print(doc['comments'])
        comment = doc['comments']
        t_list = jieba.lcut(str(comment),cut_all=False)
        for word in t_list:
            if word not in stop_set and 5>len(word)>1:
                words_list.append(word)
        words_dict = dict(Counter(words_list))

    return words_dict

def classify_frequenc(word_dict,minment=5):
    num = minment - 1
    dict = {k:v for k,v in word_dict.items() if v > num}
    return dict
def load_stopwords_set(stopwords_path):
    '''
    载入停词集
    :param stopwords_path: 停词集路径
    :return:
    '''
    stop_set = set()
    with open(str(stopwords_path),'r') as fp:
        line=fp.readline()
        while line is not None and line!= "":
            # print(line.strip())
            stop_set.add(line.strip())
            line = fp.readline()
            # time.sleep(2)
    return stop_set

def get_wordcloud(dict,title,save=False):
    '''

    :param dict: 词频字典
    :param title: 标题(电影名)
    :param save: 是否保存到本地
    :return:
    '''
    # 词云设置
    mask_color_path = "bg_1.png"  # 设置背景图片路径
    font_path = '华文黑体.ttf'  # 为matplotlib设置中文字体路径没
    imgname1 = "color_by_defualut.png"  # 保存的图片名字1(只按照背景图片形状)
    imgname2 = "color_by_img.png"  # 保存的图片名字2(颜色按照背景图片颜色布局生成)
    width = 1000
    height = 860
    margin = 2
    # 设置背景图片
    mask_coloring = imread(mask_color_path)
    # 设置WordCloud属性
    wc = WordCloud(font_path=font_path,  # 设置字体
                   background_color="white",  # 背景颜色
                   max_words=150,  # 词云显示的最大词数
                   mask=mask_coloring,  # 设置背景图片
                   max_font_size=200,  # 字体最大值
                   # random_state=42,
                   width=width, height=height, margin=margin,  # 设置图片默认的大小,但是如果使用背景图片的话,那么保存的图片大小将会按照其大小保存,margin为词语边缘距离
                   )
    # 生成词云
    wc.generate_from_frequencies(dict)

    bg_color = ImageColorGenerator(mask_coloring)
    # 重定义字体颜色
    wc.recolor(color_func=bg_color)
    # 定义自定义字体，文件名从1.b查看系统中文字体中来
    myfont = FontProperties(fname=font_path)
    plt.figure()
    plt.title(title, fontproperties=myfont)
    plt.imshow(wc)
    plt.axis("off")
    plt.show()

    if save is True:#保存到
        wc.to_file(imgname2)

if __name__ =='__main__':
    base_url = 'https://movie.douban.com/subject/26935251'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
        'Upgrade-Insecure-Requests': '1',
        'Cookie': '******',#cookie请自行修改
        'Connection':'keep-alive',
        'Upgrade-Insecure-Requests':'1',
        'Host':'movie.douban.com',
    }
    start = 0
    response = requests.get(base_url,headers)
    tree = etree.HTML(response.text)
    movie_name = tree.xpath('//div[@id="content"]/h1/span')[0].text.strip()
    # print(movie_name)

    url = base_url+'/comments'

    stopwords_path = 'stopwords.txt'
    stop_set = load_stopwords_set(stopwords_path)

    #数据库连接
    client = MongoClient('localhost', 27017)
    db = client.douban
    db.authenticate('douban_sa','sa') #mongodb服务开启没有加上 --auth 参数时 需要注释该行
    collection = db.movie_comments

    try:
        # 抓取评论 保存到数据库
        get_comments(url, headers,start, 5, movie_name,None)
        #从数据库获取评论 并分好词
        frequency_dict = get_words_frequency(collection,stop_set)
        # 对词频进一步筛选
        cl_dict = classify_frequenc(frequency_dict,5)
        # 根据词频 生成词云
        get_wordcloud(cl_dict,movie_name)
    finally:
        # pass
        client.close()



