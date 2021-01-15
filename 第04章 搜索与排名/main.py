import searchengine
import nn

# main()
# 因为网址无法访问，所以跳过爬虫和数据库建立部分，使用现成的数据库
crawler = searchengine.crawler('豆瓣电影数据库1.db')
# 如果之前创建过表，则注释crawler.createindextables()
# crawler.createindextables()
# pages = 'https://movie.douban.com/top250'
# crawler.crawl(pages)

print('<----Database is prepared!---->')
# print([row for row in crawler.con.execute( \
#     'select rowid from wordlocation where wordid = 19')])

# Querying
# print('<----Querying---->')
# e = searchengine.searcher('豆瓣电影数据库1.db')
# result = e.getmatchrows('吴孟达 周星驰')
# print(result)

#Content-Based Ranking
# print('<----Content-Based Ranking---->')
# e.query('周星驰')

# PageRank
# print('<----PageRank!---->')
# crawler.calculatepagerank()
# cur = crawler.con.execute('select * from pagerank order by score desc')
# pay attention to that 'sqlite3.Cursor' object is not subscriptable
# for i in range(5):
#     print(cur.fetchone())
# print(e.geturlname(438))

# Learning from Clicks
print('<----Learning from Clicks---->')
mynet = nn.searchnet('豆瓣电影数据库1.db')
# mynet.maketables()
# wWorld, wRiver, wBank = 101, 102, 103
# uWorldBank, uRiver, uEarth = 201, 202, 203

# mynet.generatehiddennode([wWorld, wBank], [uWorldBank, uRiver, uEarth])
# print('<-Before Training:->')
# print(mynet.getresult([wWorld, wBank], [uWorldBank, uRiver, uEarth]))
##训练需要的数据格式：关键词id（不能大于等于3个），相关联网址id，点击的网址id。
# for i in range(30):
#     # mynet.trainquery([wWorld, wBank], [uWorldBank, uRiver, uEarth], uWorldBank)
#     mynet.trainquery([656, 659], [1], 1)
#     mynet.trainquery([656], [1], 1)
#     mynet.trainquery([656, 659], [17, 1, 11], 1)
#     mynet.trainquery([656, 659], [15, 1, 11], 1)
#     mynet.trainquery([12, 656], [15, 1, 11], 1)
#     mynet.trainquery([131, 659], [15, 1, 11], 1)
#     mynet.trainquery([121, 56], [15, 2, 11], 2)
#     mynet.trainquery([162, 66], [3, 1, 11], 3)
#     mynet.trainquery([142, 656], [15, 1, 22], 22)
#     mynet.trainquery([131, 611], [15, 1, 22], 22)
#     mynet.trainquery([124, 56], [15, 22, 11], 22)
#     mynet.trainquery([162, 66], [22, 1, 11], 22)
#     mynet.trainquery([12, 656], [22, 1, 11], 22)
#     mynet.trainquery([131, 659], [15, 22, 1], 1)
#     mynet.trainquery([122, 56], [15, 22, 11], 22)
#     mynet.trainquery([161, 66], [21, 22, 11], 22)
# print('<-After Training:->')
# print(mynet.getresult([659], [1]))

'''
#找id
select rowid from wordlist where word='电影'
周星驰：656，吴孟达:659
'''
#Querying
print('<----Querying---->')
e = searchengine.searcher('豆瓣电影数据库1.db')
# 查找全部url和位置
result = e.getmatchrows('吴孟达 周星驰')
print(result)

##看哪个url更推荐
print('权重越高，url越推荐')
print(mynet.getresult([656, 659], [1, 16, 11]))
#url的分数，根据点击或者根据加权综合（pagerank，基于内容，点击）。

e.query('周星驰 吴孟达')