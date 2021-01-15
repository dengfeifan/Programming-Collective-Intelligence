import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import sqlite3
import re
import jieba
# Create a list of words to ignore
# ignorewords={'the':1,'of':1,'to':1,'and':1,'a':1,'in':1,'is':1,'it':1}
ignorewords = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it', '-', '，', '。', "'", '', u'的', u'是'])
class Crawler:
    # Initialize the crawler with the name of database
    def __init__(self, db_name):
        self.con = sqlite3.connect(db_name, timeout=10)
    
    def __del__(self):
        self.con.close()
        
    def db_commit(self):
        self.con.commit()
    
    # Axillary function for getting an entry id and adding
    # it if it's not present
    def get_entry_id(self, table, field, value, create_new=True):
        cur=self.con.execute("select rowid from %s where %s='%s'" % (table,field,value))
        res=cur.fetchone()
        if res==None:
            cur=self.con.execute("insert into %s (%s) values ('%s')" % (table,field,value))
            return cur.lastrowid
        else:
            return res[0] 
    
    # Create index for every page
    def add_to_index(self, url, soup):
        if self.is_indexed(url):
            return 
        print('Indexing %s' % url)
        
        # Get the individual words
        text = self.get_text_only(soup)
        words = self.separate_words(text)
        
        # Get the id of URL
        url_id = self.get_entry_id('urllist', 'url', url)
        
        # Link each word to this url
        for i in range(len(words)):
            word = words[i]
            if word in ignorewords:
                continue
            word_id = self.get_entry_id('wordlist','word',word)
            self.con.execute("insert into wordlocation(urlid,wordid,location) \
                             values (%d,%d,%d)" % (url_id,word_id,i))
        
    # Extract the text from an HTML page (no tags)
    def get_text_only(self, soup):
        v = soup.string
        if v==None:
            c = soup.contents
            result_text=''
            for t in c:
                sub_text = self.get_text_only(t)
                result_text += sub_text + '\n'
            return result_text
        else:
            return v.strip()
    
    # Separate words by any non_whitespace character
#     def separate_words(self, text):
#         splitter = re.compile('\\W*')
#         return [s.lower() for s in splitter.split(text) if s!='']
    def separate_words(self, text):
        seg_list = jieba.cut_for_search(text)
        result = []
        for seg in seg_list:
            if seg in ignorewords:
                continue
            else:
                result.append(seg)
        return result
    
    # Return true if the url is already indexed 
    def is_indexed(self, url):
        u = self.con.execute("select rowid from urllist where url='%s'" % url).fetchone()
        if u != None:
            v = self.con.execute("select * from wordlocation where urlid=%d" % u[0]).fetchone()
            if v != None:
                return True
        return False
    
    # Add a link between two pages
    def add_link_ref(self, urlFrom, urlTo, linkText):
        words = self.separate_words(linkText)
        from_id = self.get_entry_id('urllist', 'url', urlFrom)
        to_id = self.get_entry_id('urllist', 'url', urlTo)
        if from_id == to_id:
            return
        cur = self.con.execute('insert into link(fromid, toid) values (%d, %d)' % (from_id, to_id))
        link_id = cur.lastrowid
        for word in words:
            if word in ignorewords:
                continue
            word_id = self.get_entry_id('wordlist', 'word', word)
            self.con.execute("insert into linkwords(linkid,wordid) values (%d,%d)" % (link_id,word_id))
    
    # Starting with a list of pages, do a breadth 
    # first search to the given depth, indexing pages 
    # as we go
    def crawl(self, pages, depth=2):
        for i in range(depth):
            print('depth %d begins' % i)
            new_pages = set()
            herders={
    'User-Agent':'Mozilla/5.0 (Windows NT 6.1;WOW64) AppleWebKit/537.36 (KHTML,like GeCKO) Chrome/45.0.2454.85 Safari/537.36 115Broswer/6.0.3',
    'Referer':'https://movie.douban.com/',
    'Connection':'keep-alive'}
            for page in pages:
                try:
                    
                    req=urllib.request.Request(page,headers=herders)
                    c=urllib.request.urlopen(req)
                    
#                     c = urllib.request.urlopen(page)
                except:
                    print('Could not open %s' % page)
                    continue
                soup = BeautifulSoup(c.read(),'lxml')
                self.add_to_index(page, soup)
                
                links = soup('a')
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url = urllib.parse.urljoin(page, link['href'])
                        if url.find("'")!=-1:
                            continue
                        url=url.split('#')[0]  # remove location portion
                        if url[0:4]=='http' and not self.is_indexed(url):
                            if self.urlfilter(url):
                                  new_pages.add(url)
                                  link_text = self.get_text_only(link)
                                  self.add_link_ref(page, url, link_text)
                self.db_commit()
            pages = new_pages
    
    # Create the database tables
    def create_index_tables(self):
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid,wordid,location)')
        self.con.execute('create table link(fromid integer,toid integer)')
        self.con.execute('create table linkwords(wordid,linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordurlidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.db_commit()
        
    def calculate_pagerank(self, iterations=20):
        # clear out the current PageRank tables
        self.con.execute('drop table if exists pagerank')
        self.con.execute('create table pagerank(urlid primary key, score)')
        
        # Initialize every url with a pagerank of 1
        self.con.execute('insert into pagerank select rowid, 1.0 from urllist')
        self.db_commit()
        
        for i in range(iterations):
            print('Iteration %d' % i)
            for (url_id,) in self.con.execute('select rowid from urllist'):
                pr = 0.15
                
                # Loop through all the pages that link to this one
                for (linker,) in self.con.execute('select distinct fromid from link where toid=%d' % url_id):
                    # Get the pagerank of the linker 
                    linking_pr = self.con.execute('select score from pagerank where urlid=%d' % linker).fetchone()[0]
                    
                    # Get the total number of links from the linker
                    linking_count = self.con.execute('select count(*) from link where fromid=%d' % linker).fetchone()[0]
                    pr += 0.85*(linking_pr/linking_count)
                self.con.execute('update pagerank set score=%f where urlid=%d' % (pr, url_id))
            self.db_commit()

# ##豆瓣正则化过滤url网址（筛选需要的网址）
    def urlfilter(self, url):
        matcher_1 = re.compile(r'http://movie.douban.com/top250\?start=\d+.*')
        matcher_2 = re.compile(r'^https://movie.douban.com/subject/\d+/$')
        if (matcher_1.match(url) is not None or
                matcher_2.match(url) is not None):
            return True
        return False
if __name__=='__main__':
    crawler = Crawler('豆瓣电影数据库1.db')
# Run create_index_tables when db hasn't been created
    crawler.create_index_tables()
    page_list = ['https://movie.douban.com/top250']
    crawler.crawl(page_list)