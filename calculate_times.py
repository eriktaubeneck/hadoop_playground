import sqlite3
import time
from collections import defaultdict

con = sqlite3.connect('zulip.db')
cur = con.cursor()

cur.execute('SELECT timestamp FROM messages ORDER BY timestamp;')
con.close()

times = cur.fetchall() #Gets returned as a list of tuples in unix time

unix_times = [item[0] for item in times]
local_times = [time.localtime(item) for item in unix_times]

base = [0]*24
b_times =[[0 for _ in xrange(24)] for _ in xrange(7)]
days_recorded = [[set() for _ in xrange(24)] for _ in xrange(7)]
for t in local_times:
#Day of the week is indexed with monday=0
    day = t.tm_wday
    hour = t.tm_hour
    yearday = t.tm_yday
    b_times[day][hour] += 1
    (days_recorded[day][hour]).add(yearday)




#We probably want to nest day of the week and then hour of the day.




