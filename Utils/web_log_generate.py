#coding=UTF-8
 
import random
import time
import sys
 
url_paths = [
    "class/112.html",
    "class/128.html",
    "class/145.html",
    "class/146.html",
    "class/131.html",
    "class/130.html",
    "learn/821.html",
    "course/list"
]
 
ip_slices = [
    132,156,124,10,29,167,143,187,30,46,55,63,72,87,98,168
]
 
# 跳转
http_referers = [
    "http://www.baidu.com/s?wd={query}",
    "http://www.sogou.com/web?query={query}",
    "http://cn.bing.com/search?q={query}",
    "http://www.yahoo.com/search?p={query}"
]
 
search_keyword = [
    "Spark SQL实战",
    "Hadoop基础",
    "Storm实战",
    "Spark Streaming实战",
    "理论"
]
 
status_codes = ["200","404","500"]
 
# 生成URL
def sample_url():
    return random.sample(url_paths,1)[0]
 
# 生成ip
def samle_ip():
    slice = random.sample(ip_slices,4)
    return ".".join([str(item) for item in slice])
 
# 生成referer
def sample_referer():
    if random.uniform(0,1) > 0.2:
        return "-"
    refer_str = random.sample(http_referers,1)
    query_str = random.sample(search_keyword,1)
    return  refer_str[0].format(query=query_str[0])
 
# 生成状态码
def sample_status_code():
    a = random.normalvariate(1000, 200)
    if a < 1200 :
        return status_codes[0]
    elif a < 1400:
        return status_codes[1]
    else:
        return status_codes[2]
    # return random.sample(status_codes,1)[0]

def log():
    time_str = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
    return "{ip}\t{local_time}\t\"GET /{url} HTTP/1.1\"\t{status_code}\t{referer}".format(url=sample_url(),ip=samle_ip(),referer=sample_referer(),status_code=sample_status_code(),local_time=time_str)
 
def generate_log(count = 10): 
    #要写入的文件 该文件要先在服务器上创建好touch generateLog.log 代码在服务器上面跑
    f = open("/tmp/generateLog.log","w+")
    while count >= 1:
        query_log = log()
        print(query_log)
 
        f.write(query_log + "\n")
        count = count - 1

def forever(t, file = None):
    if file:
        file = open(file,"w+")
    while True:
        query_log = log()
        print(query_log)
        if file:
            file.write(query_log + "\n")
            file.flush()
        sleeptime = random.normalvariate(t, t/4) / 1000
        time.sleep(sleeptime)

if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) < 2:
        generate_log(100)
    elif len(sys.argv) == 2:
        forever(int(sys.argv[1]), None)
    elif len(sys.argv) == 3:
        forever(int(sys.argv[1]), sys.argv[2])
