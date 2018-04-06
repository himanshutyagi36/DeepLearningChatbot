import sqlite3
import json
from datetime import datetime

timeframe = '2015-05'
sql_transaction = []
start_row = 0
cleanup = 1000000

## create sqlite connection
connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

## following code created a table wiht the defined columns
def create_table():
    c.execute(""" CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, 
    comment TEXT, subreddit TEXT, unix INT, score INT ) """)

## format the data. replace \n and \r. replace all " with '
def format_data(data):
    data = data.replace('\n',' newlnechar ').replace('\r', ' newlinechar ').replace('"',"'")
    return data

def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id='{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else: return False
    except Exception as e:
        # print("find_parent", e)
        return False

# here we define acceptable data for training. 
# number of characters ranging 1 to 50
# if more/less than that, we ignore this data. Also, if the comment is deleted or removed
# we don't consider this data for traning
def acceptable(data):
    if len(data.split(' ')) >50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]':
        return False
    elif data == '[removed]':
        return False
    else:
        return True


# Initially all comments will not have a parent, because each comment may be top level
# comment or because the parent isn't in the document. But, as we go through the document, we will eventually find 
# comments that have parent in this database.
# if we have a comment_id in our database that matches another comment's parent_id, 
# then we should match this new comment with the parent that we have already. 
def find_parent(pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id='{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else: return False
    except Exception as e:
        # print("find_parent", e)
        return False


def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []

def sql_insert_replace_comment(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? 
        WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s-INSERT insertion',str(e))

def sql_insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) 
        VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-PARENT insertion',str(e))

def sql_insert_no_parent(commentid,parentid,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) 
        VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s-NO_PARENT insertion',str(e))


if __name__ == "__main__":
    create_table()
    row_counter = 0
    paired_rows = 0 ## tells us comment and reply pairs, which are training data

    with open('dataset/{}/RC_{}'.format(timeframe.split('-')[0],timeframe), buffering=1000) as f:
        for row in f:
            row_counter +=1

            if row_counter > start_row:
                try:
                    row = json.loads(row)
                    parent_id = row['parent_id']
                    body = format_data(row['body'])
                    created_utc = row['created_utc']
                    score = row['score']
                    subreddit = row['subreddit']
                    parent_data = find_parent(parent_id)
                    # comment_id = row['id']
                    comment_id = row['name']
                                
                    existing_comment_score = find_existing_score(parent_id)
                    # if there is an existing comment for this parent comment,
                    # and the score of current is higher than existing, use current.
                    if existing_comment_score:
                        if score > existing_comment_score:
                            if acceptable(body):
                                sql_insert_replace_comment(comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                        else:
                            if acceptable(body):
                                if parent_data:
                                    if score >=2:
                                        sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                                        paired_rows += 1
                                else:
                                    sql_insert_no_parent(comment_id,parent_id,body,subreddit,created_utc,score)
                except Exception as e:
                    print('for loop',str(e)) 

            if row_counter % 100000 == 0:
                print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(row_counter, paired_rows, str(datetime.now())))

            if row_counter > start_row:
                if row_counter % cleanup == 0:
                    print("Cleanin up!")
                    sql = "DELETE FROM parent_reply WHERE parent IS NULL"
                    c.execute(sql)
                    connection.commit()
                    c.execute("VACUUM")
                    connection.commit()
            

            