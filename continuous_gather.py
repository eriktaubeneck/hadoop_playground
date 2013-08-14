import humbug
import json
import sqlite3


filename = 'messages2.txt'
client = humbug.Client(email='hharvest-bot@students.hackerschool.com', api_key='BhEUebvGYxq3m1WK7T0u43ohQof0fNTV')

first_id = 0
max_id = 0
q_param = {}
n_mess = 1000000
DB = True

if DB:
    con = sqlite3.connect('zulip.db')
    cur = con.cursor()

def add_to_db(msg):
    user_id = int(msg['sender_id'])
    name = msg['sender_full_name']
    email = msg['sender_email']
    msg_id = int(msg['id'])
    content = msg['content']
    stream = msg['display_recipient']
    stream_id = int(msg['recipient_id'])
    subject = msg['subject']
    time = msg['timestamp']
    email = msg['sender_email']

    try:
        cur.execute('INSERT INTO messages VALUES (?,?,?,?,?,?);', (msg_id, time, user_id, stream_id, subject, content))
    except sqlite3.IntegrityError:
        pass

    try:
        cur.execute('INSERT INTO users (id, long_name, email) VALUES (?, ?, ?);', (user_id, name, email))
    except sqlite3.IntegrityError:
        pass
    con.commit()


while max_id - first_id < n_mess:
    try:
        if max_id:
            q_param = {"last":max_id}
        r = client.get_messages(q_param)
        if r['messages']:
            max_id = max([m['id'] for m in r['messages']])
            if not first_id:
                first_id = min([m['id'] for m in r['messages']])
            with open(filename, 'a') as f:
                for m in r['messages']:
                    print "Got MESSAGE: {}".format(m['id'])
                    json.dump(m, f)
                    f.write('\n')
                if DB:
                    add_to_db(m)
    except Exception as e:
        print e

print "DONE NNENENENE"
