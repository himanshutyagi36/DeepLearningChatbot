import sqlite3
import json
from datetime import datetime

timeframe = '2018-02'
sql_transaction = []


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

if __name__ == "__main__":
    create_table()
    row_counter = 0
    paired_rows = 0 ## tells us comment and reply pairs, which are training data

    with open('dataset/{}/RC_{}'.format(timeframe.split('-')[0],timeframe), buffering=1000) as f:
        for row in f:
            row_counter +=1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            # comment_id = row['name']
            subreddit = row['subreddit']

            parent_data = find_parent(parent_id)
            

            