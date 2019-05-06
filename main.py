import pymysql, math, operator, json
from nltk.tokenize import word_tokenize

fp = open("database.json")
config = json.load(fp)
fp.close()

conn = pymysql.connect(host = config['host'], port = config['port'], user = config['user'], password=config['password'], db = config['database'])
cursor = conn.cursor()

def write(filename, content):
    with open(filename, "a") as f:
        f.write(content)

#============================================================
#              Creating Inverted Index Table
#============================================================
cursor.execute('SELECT * FROM wiki')
results = cursor.fetchall()

cursor.execute('DROP TABLE IF EXISTS `InvIdx`')
cursor.execute('CREATE TABLE `InvIdx` ( `term` varchar(1000) NOT NULL, `id` int(11) NOT NULL ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')

#================Inserting Inverted Index====================
cursor.execute("LOCK TABLES InvIdx WRITE")

InvIdxTable = {}
query = "INSERT INTO InvIdx(term, id) VALUES (%s, %s)"
query_tuples = []
n_d = {}
n_d_t = {}
id_title = {}

for result in results:
    id_title.update({result[0]: result[1]})
    id = result[0]
    terms = word_tokenize(result[1]) + word_tokenize(result[2])            
    total_terms = 0
    n_d_elem = {}

    for term in terms:
        if True:
            total_terms += 1
            if not term in n_d_elem:
                n_d_elem.update({term : 1})
            else:
                num = n_d_elem[term]
                num += 1
                n_d_elem.update({term : num})

            if not term in InvIdxTable:
                InvIdxTable.update({term: [id]})
                query_tuples.append([term, id])
            else:
                InvElem = InvIdxTable[term]
                if not id in InvElem:
                    InvElem.append(id)
                    InvIdxTable.update({term:InvElem})
                    query_tuples.append([term, id])

    n_d_t.update({id : n_d_elem})
    n_d.update({id : total_terms})

cursor.executemany(query, query_tuples)
cursor.execute("UNLOCK TABLES")
        
#============================================================
#                      TFIDF Calculating
#============================================================

TFIDF = {}
for title in n_d_t:
    TFIDF_ELEM = {}
    elem = n_d_t[title]
    for term in elem:
        tf_idf_value = math.log(1 + n_d_t[title][term]/n_d[title]) / len(InvIdxTable[term])
        TFIDF_ELEM.update({term : tf_idf_value})
    TFIDF.update({title : TFIDF_ELEM})

#============================================================

cursor.execute('SELECT * FROM link')
results = cursor.fetchall()
        
PageRank = {}
PageLink = {}
PageLink_inv = {}

for result in results:
    if result[0] in PageLink:
        elem = PageLink[result[0]]
        elem.append(result[1])
        PageLink.update({result[0] : elem})
    else:
        elem = [result[1]]
        PageLink.update({result[0] : elem})

    if not result[0] in PageRank:
        PageRank.update({result[0] : 0})

    if not result[1] in PageRank:
        PageRank.update({result[1] : 0})

    if result[1] in PageLink_inv:
        elem = PageLink_inv[result[1]]
        elem.append(result[0])
        PageLink_inv.update({result[1] : elem})
    else:
        elem = [result[0]]
        PageLink_inv.update({result[1] : elem})
        
#============================================================
#                      PageRank Calculating
#============================================================

epsilon = 10.0
total_page = len(PageRank)
for page in PageRank:
    PageRank.update({page : 1/total_page})

num_iter = 0
while epsilon > 10e-8:
    num_iter += 1
    epsilon = 0
    for id in PageRank:
        if id in PageLink_inv:
            prev_value = PageRank[id]
            term1 = 0.15/total_page
            term2 = 0
            for linked_id in PageLink_inv[id]:
                term2+=1/len(PageLink[linked_id]) * PageRank[linked_id]
            post_value = (term1 + (1-0.15) * term2)
            epsilon += abs(post_value - prev_value)
            PageRank.update({id : post_value})
        
conn.commit()
conn.close()
print('building tables...')
print('ready to search')    

while True:
    terms  = input("2018-22788>").split()
    terms2 = []

    if terms[0] == "-run":
        logfile = terms[1]
    else:
        for term in terms:
            for word in InvIdxTable:
                if term != word and term.lower() == word.lower():
                    terms2.append(word)
        terms = terms + terms2
        terms = list(set(terms))

        result_tfidf = {}
        for term in terms:
            if term in InvIdxTable:
                for page_id in InvIdxTable[term]:
                    if not int(page_id) in result_tfidf:
                        result_tfidf.update({int(page_id) : TFIDF[page_id][term]})
                    else:
                        sum_v = result_tfidf[int(page_id)] + TFIDF[page_id][term]
                        result_tfidf.update({int(page_id): sum_v})

        result = {}
        for id in result_tfidf:
            result.update({id : -result_tfidf[id] * PageRank[id]})

        result = sorted(result.items(), key = operator.itemgetter(1,0))
        for i in range(10):
            if i > len(result) - 1:
                break
            print(result[i][0],',',id_title[result[i][0]],',',result_tfidf[result[i][0]],',',PageRank[result[i][0]])


