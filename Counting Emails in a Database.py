"""
This application will read the mailbox data ("https://www.py4e.com/code3/mbox.txt")
and count the number of email messages per organization (i.e. domain name of
the email address) using a database with the following schema to maintain the
counts.
"""

import sqlite3
from urllib.request import Request, urlopen

#Connecting to the file in which we want to store our db
conn = sqlite3.connect('emaildb.sqlite')
cur = conn.cursor()

#Initializing
cur.execute('''
DROP TABLE IF EXISTS Counts''')

#Creating ttable
cur.execute('''
CREATE TABLE Counts (org TEXT, count INTEGER)''')

# using link to stream data from
url="https://www.py4e.com/code3/mbox.txt"
# Bypass security layer through header. Otherwise 403 Http error
req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
fname = urlopen(req).read().decode()
# Writing string to a local text file
textfile = open('textfile.txt', 'w')
textfile.write(fname)
textfile.close()

# Reading the previosuly written file
fh = open('textfile.txt')

#Reading each line of the file
for line in fh:

    #Finding an email address and splitting it into name and organization
    if not line.startswith('From: ') : continue
    pieces = line.split()
    email = pieces[1]
    (emailname, organization) = email.split("@")


    #Updating the table with the correspondent information
    cur.execute('SELECT count FROM Counts WHERE org = ? ', (organization, ))
    row = cur.fetchone()
    if row is None:
        cur.execute('''INSERT INTO Counts (org, count)
                VALUES ( ?, 1 )''', ( organization, ) )
    else :
        cur.execute('UPDATE Counts SET count=count+1 WHERE org = ?',
            (organization, ))

#Commit
conn.commit()

# Getting the top 10 results and showing them
sqlstr = 'SELECT org, count FROM Counts ORDER BY count DESC LIMIT 10'
print
print ("Counts:")
for row in cur.execute(sqlstr) :
    print (str(row[0]), row[1])

#Closing the DB
cur.close()
