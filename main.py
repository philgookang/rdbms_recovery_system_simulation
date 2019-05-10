import pymysql, math, operator, json, re, os
from nltk.tokenize import word_tokenize
from urllib.parse import quote_plus, unquote_plus

fp = open("database.json")
config = json.load(fp)
fp.close()

conn = pymysql.connect(host = config['host'], port = config['port'], user = config['user'], password=config['password'], db = config['database'])
cursor = conn.cursor()

def homework_one( cursor ):
    # ============================================================
    #              Creating Inverted Index Table
    # ============================================================

    cursor.execute('SELECT * FROM wiki WHERE status=%s', tuple([1]))
    results = cursor.fetchall()

    cursor.execute('DROP TABLE IF EXISTS `InvIdx`')
    cursor.execute('CREATE TABLE `InvIdx` ( `term` varchar(1000) NOT NULL, `id` int(11) NOT NULL ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')

    # ================Inserting Inverted Index====================

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
                    n_d_elem.update({term: 1})
                else:
                    num = n_d_elem[term]
                    num += 1
                    n_d_elem.update({term: num})

                if not term in InvIdxTable:
                    InvIdxTable.update({term: [id]})
                    query_tuples.append([term, id])
                else:
                    InvElem = InvIdxTable[term]
                    if not id in InvElem:
                        InvElem.append(id)
                        InvIdxTable.update({term: InvElem})
                        query_tuples.append([term, id])

        n_d_t.update({id: n_d_elem})
        n_d.update({id: total_terms})

    cursor.executemany(query, query_tuples)
    cursor.execute("UNLOCK TABLES")

    # ============================================================
    #                      TFIDF Calculating
    # ============================================================

    TFIDF = {}
    for title in n_d_t:
        TFIDF_ELEM = {}
        elem = n_d_t[title]
        for term in elem:
            tf_idf_value = math.log(1 + n_d_t[title][term] / n_d[title]) / len(InvIdxTable[term])
            TFIDF_ELEM.update({term: tf_idf_value})
        TFIDF.update({title: TFIDF_ELEM})

    # ============================================================

    cursor.execute('SELECT * FROM link WHERE status=%s', tuple([1]))
    results = cursor.fetchall()

    PageRank = {}
    PageLink = {}
    PageLink_inv = {}

    for result in results:
        if result[0] in PageLink:
            elem = PageLink[result[0]]
            elem.append(result[1])
            PageLink.update({result[0]: elem})
        else:
            elem = [result[1]]
            PageLink.update({result[0]: elem})

        if not result[0] in PageRank:
            PageRank.update({result[0]: 0})

        if not result[1] in PageRank:
            PageRank.update({result[1]: 0})

        if result[1] in PageLink_inv:
            elem = PageLink_inv[result[1]]
            elem.append(result[0])
            PageLink_inv.update({result[1]: elem})
        else:
            elem = [result[0]]
            PageLink_inv.update({result[1]: elem})

    # ============================================================
    #                      PageRank Calculating
    # ============================================================

    epsilon = 10.0
    total_page = len(PageRank)
    for page in PageRank:
        PageRank.update({page: 1 / total_page})

    num_iter = 0
    while epsilon > 10e-8:
        num_iter += 1
        epsilon = 0
        for id in PageRank:
            if id in PageLink_inv:
                prev_value = PageRank[id]
                term1 = 0.15 / total_page
                term2 = 0
                for linked_id in PageLink_inv[id]:
                    term2 += 1 / len(PageLink[linked_id]) * PageRank[linked_id]
                post_value = (term1 + (1 - 0.15) * term2)
                epsilon += abs(post_value - prev_value)
                PageRank.update({id: post_value})

    return InvIdxTable, TFIDF, PageRank, id_title

def search_terms(terms, InvIdxTable, TFIDF, PageRank, id_title):

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
                    result_tfidf.update({int(page_id): TFIDF[page_id][term]})
                else:
                    sum_v = result_tfidf[int(page_id)] + TFIDF[page_id][term]
                    result_tfidf.update({int(page_id): sum_v})

    result = {}
    for id in result_tfidf:
        a = -result_tfidf[id]
        # if id == 24031233:
        #     gfff = 1
        #     gff3 = 1
        b = 0
        if id in PageRank:
            b = PageRank[id]
        result.update({id: a * b})

    result = sorted(result.items(), key=operator.itemgetter(1, 0))
    print_list = []

    for i in range(10):
        if i > len(result) - 1:
            break
        s = str(result[i][0]) + ',' + str(id_title[result[i][0]]) + ',' + str(result_tfidf[result[i][0]]) + ',' + str(PageRank[result[i][0]])
        print_list.append(s)

    return print_list

def write_to_log(content):
    with open("prj2.log", "a") as f:
        f.write(content + "\n")

def write_to_recovery(content):
    with open("recovery.txt", "a") as f:
        f.write(content + "\n")

def write_to_search(content):
    with open("search.txt", "a") as f:
        f.write(content + "\n")

def rollback(tran_id, cursor):
    logs = []
    transaction_action_list = []
    with open("prj2.log", "r") as f:
        for line in f:
            logs.append( (line[1:-2]).split(",") )

    for log in reversed(logs):
        if log[0] == tran_id:
            if log[1] == 'start': break
            elif log[1] == 'abort': continue
            transaction_action_list.append(log)

    for transaction in transaction_action_list:
        undo_log(cursor, transaction)

def recover(cursor, num):
    logs = []
    active_list = { }
    completed_list = { }

    with open("prj2.log", "r") as f:
        for line in f: logs.append(((line[1:-2]).split(",")))

    checkpoint_index = 0
    for index,log in enumerate(reversed(logs)):
        if log[0] == 'checkpoint':
            checkpoint_index = index + 1

            # add active list
            for i in range(1, len(log)):
                active_list[log[i]] = 1

            break

    # write recovery line
    write_to_recovery("recover "+str((num+1)))

    start = len(logs) - checkpoint_index
    if checkpoint_index == 0:
        start = 0

    # redo!!!!
    for i in range( start, len(logs) ):
        log = logs[i]
        if log[0] == "checkpoint":
            continue # <-------------------------------------------
        elif log[1] == 'start':
            active_list[log[0]] = 1
        elif log[1] == 'abort':

            # call rollback recursivelty
            rollback(log[0], cursor)

            completed_list[log[0]] = 1
            del active_list[log[0]]
        elif log[1] == 'commit':
            completed_list[log[0]] = 1
            del active_list[log[0]]
        else:
            redo_log(cursor, log)

    keys = ""
    for i,key in enumerate(completed_list):
        if i: keys += ","
        keys += key
    write_to_recovery("redo " + keys )

    keys = ""
    for i,key in enumerate(active_list):
        if i: keys += ","
        keys += key
    write_to_recovery("undo " + keys)

    # undo!!!!
    for i in range( (len(logs) - 1) , -1, -1):

        # no more active transaction
        if len(active_list) == 0:
            break

        log = logs[i]

        if log[0] in active_list:
            # found trasnaction starting point so remove it from list
            if log[1] == 'start':
                del active_list[log[0]]
                continue
            undo_log(cursor, log)

def redo_log(cursor, transaction):
    sql_info = transaction[1].split('.')
    if sql_info[3] == 'delete':
        cursor.execute('UPDATE {0} SET status=0 WHERE {1}={2}'.format(sql_info[0], sql_info[2], sql_info[1]))

    elif sql_info[3] == 'update':
        query = "UPDATE {0} SET {1}=%s WHERE id=%s".format(sql_info[0], sql_info[2])
        cursor.execute(query, tuple([unquote_plus(transaction[3]), sql_info[1]]))

def undo_log(cursor, transaction):
    sql_info = transaction[1].split('.')
    if sql_info[3] == 'delete':
        cursor.execute('UPDATE {0} SET status=1 WHERE {1}={2}'.format(sql_info[0], sql_info[2], sql_info[1]))

    elif sql_info[3] == 'update':
        query = "UPDATE {0} SET {1}=%s WHERE id=%s".format(sql_info[0], sql_info[2])
        cursor.execute(query, tuple([unquote_plus(transaction[2]), sql_info[1]]))

if os.path.exists("prj2.log"): os.remove("prj2.log")
if os.path.exists("recovery.txt"): os.remove("recovery.txt")
if os.path.exists("search.txt"): os.remove("search.txt")

InvIdxTable, TFIDF, PageRank, id_title = homework_one(cursor)

print('building tables...')
print('ready to search')

cursor.execute('UPDATE link SET status=1')
cursor.execute('UPDATE wiki SET status=1')

while True:
    terms  = input("2018-22788>").split()
    terms2 = []
    # terms = '-run prj2.sched'.split()
    active_transaction_list = {  }

    if terms[0] == "-run":
        logfile = terms[1]
        with open(logfile, "r") as fp:
            for num,line in enumerate(fp):
                line = line.strip()
                if line == 'system failure - recover':
                    recover(cursor, num)
                    InvIdxTable, TFIDF, PageRank, id_title = homework_one(cursor)
                    # =====
                    active_transaction_list = { }
                elif line == 'checkpoint':
                    k = ""
                    for i,key in enumerate(active_transaction_list):
                        if i: k += ","
                        k += key
                    if k != "":
                        k= ","+k
                    write_to_log('<checkpoint{0}>'.format(k))
                else:
                    split_record = line.split(' ')
                    if split_record[0] == 'search':
                        t = split_record.copy()
                        t.pop(0)

                        qq = ""
                        for word in t: qq += word + " "

                        write_to_search("search {0}".format( str((num+1)) ))
                        write_to_search("query {0}".format(qq))

                        ll = search_terms(t, InvIdxTable, TFIDF, PageRank, id_title)

                        for l in ll:
                            write_to_search(l)
                    else:
                        match               = re.search("<T(.*?)>", line)
                        transaction_number  = match.group(0).replace('<', '').replace('>', '')
                        sql_query           = line.replace('<'+transaction_number+'>', '').strip()
                        sql_query_lower     = sql_query.lower()

                        # transaction finish: record commit
                        if sql_query == 'commit':
                            del active_transaction_list[transaction_number]
                            write_to_log('<{0},commit>'.format(transaction_number))

                        # transaction abort: record abort
                        elif sql_query == 'rollback':
                            rollback(transaction_number, cursor)

                            del active_transaction_list[transaction_number]
                            write_to_log('<{0},abort>'.format(transaction_number))

                        # transaction executes
                        else:
                            # transaction first execution: record start
                            if transaction_number not in active_transaction_list:
                                active_transaction_list[transaction_number] = '1'
                                write_to_log('<{0},start>'.format(transaction_number))

                            # get table name
                            match_table_name    = re.search("(from(.*?)where)|(update(.*?)set)", sql_query_lower)
                            table_name          = match_table_name.group(0).replace('from', '').replace('where', '').replace('update', '').replace('set', '').strip()

                            # for update, we need to get old value first
                            key = ((sql_query.split('='))[-1]).strip(';').strip()

                            if 'update' in sql_query_lower:

                                column           = 'title' if 'title' in sql_query_lower else 'text'
                                match_new_value  = re.search("'(.*?)'", sql_query_lower)
                                newValue         = (match_new_value.group(0))[1:-1]

                                query = 'SELECT {0} FROM {1} WHERE id={2}'.format(column, table_name, key)
                                cursor.execute(query)
                                oldValue = (cursor.fetchone())[0]


                                cursor.execute(sql_query)    # <- query in sched file
                                write_to_log('<{0},{1}.{2}.{3}.update,{4},{5}>'.format(transaction_number, table_name, key, column, quote_plus(oldValue), quote_plus(newValue)))

                            elif 'delete' in sql_query_lower:

                                column = 'id'
                                if 'id_to' in sql_query_lower:
                                    column = 'id_to'
                                elif 'id_from' in sql_query_lower:
                                    column = 'id_from'

                                cursor.execute('UPDATE {0} SET status=0 WHERE {1}={2}'.format(table_name, column, key))
                                write_to_log('<{0},{1}.{2}.{3}.delete>'.format(transaction_number, table_name, key, column))
    else:
        ll = search_terms(terms, InvIdxTable, TFIDF, PageRank, id_title)
        for l in ll:
            print(l)
