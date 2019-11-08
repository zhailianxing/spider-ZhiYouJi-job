import  requests
import  random
from bs4 import BeautifulSoup
from lxml import etree
import  re
import  pymongo
import redis

pool = redis.ConnectionPool(host='localhost', port=6379)
red = redis.Redis(connection_pool=pool)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["job"]
mycol = mydb["jobDetail"]

def save_mongo(contents):
    mycol.insert_many(contents) # 插入1条:insert_one

user_agent_list = [
    # "Mozilla/5.0 (Windows NT 10.0; …) Gecko/20100101 Firefox/61.0", #   会报: latin1...什么错误
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
    "Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10.5; en-US; rv:1.9.2.15) Gecko/20110303 Firefox/3.6.15"
]

headers = {
        "user-agent": random.choice(user_agent_list)
    }

# work_types = ["PHP", "Golang", "Python", "Java", "C++", "C#", "C", ".NET", "Node.js", "数据采集",
#               "前端开发", "web前端", "JavaScript", "HTML5", "Flash开发"
#               ]
#  ok: PHP,golang,Python，前端开发, web前端, Node.js, Java
# TODO: 全栈工程师，后端工程师，前端工程师，技术总监
# 抓 3-5天
# ing:
work_types = ["Golang"]
for query_work in work_types:
    base_url = "https://www.jobui.com/jobs?cityKw=%E5%85%A8%E5%9B%BD&jobKw={}"
    page = 1
    is_over = False
    while page < 100 and is_over == False:
        url = ""
        if page == 1:
            url = base_url.format(query_work)
        else:
            url = base_url.format(query_work) + "&n=" + str(page)
        # 判断是否已经抓过
        redis_key = "page_url:" + url
        exist = red.get(redis_key)
        if exist != None and exist.decode("utf-8") == 1:
            continue
        try:
            response = requests.get(url, headers=headers)
        except:
            print("url:{} get error".format(url))
            page = page + 1
            continue
        finally:
            pass
        # print(response.text)
        html = etree.HTML(response.text)
        list = html.xpath('//div[@class="job-content"]')
        print("page:{},url:{},len(list):{}".format(page,url,len(list)))

        #  判断是否到最后一页:
        current_last_page = html.xpath('//div[@class="pager cfix"]/a[last()]/text()')
        print("current_last_page:",current_last_page)
        if len(current_last_page) >= 1 and current_last_page[0] != "下一页" and  page >= int(current_last_page[0]):
            print("over, now request page:",page)
            is_over = True

        for item in list:
            contents = []
            job_href = title = year = eduction = salary = company = industry =  area = detail_text_deal_str = ""
            job_hrefs = titles = years = eductions = salarys = companys = industrys = areas = []
            job_segmetation = item.xpath('div[@class="job-segmetation"]')  # div前面什么也不加，就是获取当前子节点。很重要
            if len(job_segmetation) == 4 :
                job_hrefs = job_segmetation[0].xpath('a/@href')

                titles = job_segmetation[0].xpath('a/h3/strong/text()')
                if len(titles) == 0:
                    titles = job_segmetation[0].xpath('a/h3/text()')

                years = job_segmetation[1].xpath('div/span[1]/text()')
                eductions = job_segmetation[1].xpath('div/span[2]/text()')
                salarys = job_segmetation[1].xpath('div/span[3]/text()')

                companys = job_segmetation[2].xpath('a/text()')
                industrys = job_segmetation[3].xpath('span/text()')

                print(job_hrefs, titles, years, eductions, salarys, companys, industrys)
                if len(job_hrefs) >=1 and len(titles) >=1 and len(years) >=1 and len(eductions) >=1 and len(salarys) >=1 and len(companys) >=1 and len(industrys) >=1:
                    job_href = job_hrefs[0]
                    title = titles[0]
                    year = years[0]
                    eduction = eductions[0]
                    salary = salarys[0]
                    company = companys[0]

                    industry_text_str = ''.join(industrys)
                    industry_text_deal_str = re.sub("\n", '', industry_text_str)
                    industry_text_deal_str = re.sub(" ", '', industry_text_deal_str)
                    industry = industry_text_deal_str.strip()
                    print("industry:",industry)

                    job_href = job_href[:len(job_href)-1]
                    detail_url = "https://www.jobui.com" +job_href

                    print("detail_url:",detail_url)
                    detail_redis_key = "detail_url:" + detail_url
                    exist = red.get(detail_redis_key)
                    print("detail_redis_key:",detail_redis_key)
                    if exist != None and str(exist.decode("utf-8")) == str(1):
                        continue
                    try:
                        detail_response = requests.get(detail_url, headers=headers)
                        detail_html = etree.HTML(detail_response.text)
                        detail_text = detail_html.xpath('//div[@class="hasVist cfix sbox fs16"]/text()')
                        if len(detail_text) <= 0:
                            detail_text = detail_html.xpath('//div[@class="bmsg job_msg inbox"]/text()')
                        detail_text_str = ''.join(detail_text)
                        detail_text_deal_str = re.sub("\n", '', detail_text_str)
                        detail_text_deal_str = re.sub("\t", '', detail_text_deal_str)
                        detail_text_deal_str = re.sub("\r", '', detail_text_deal_str)
                        detail_text_deal_str = detail_text_deal_str.strip()
                    except:
                        pass
                    finally:
                        pass

                    if len(detail_text_deal_str) <= 0:  #   说明是跳转到其他网址的
                        continue
                    # area
                    areas = detail_html.xpath('//ul[@class="laver cfix fs16"]/li[1]/text()')
                    if len(areas) >= 1 :
                        area = areas[0]

                    red.set(detail_redis_key, 1)

                d = {
                    "worker_type": query_work,
                    "job_desc": detail_text_deal_str,
                    "title": title,
                    "year": year,
                    "eduction": eduction,
                    "salary": salary,
                    "company": company,
                    "industry": industry,
                    "area": area,
                    "page":page,
                    "from":1   # 1表示"职友集"
                }
                contents.append(d)

            save_mongo(contents)
            # break  # test
        #页面 + 1
        print("url:{} crawl over.".format(url))
        page = page + 1
        # 记录此query查询结束
        red.set(redis_key, 1)
        # break # test



