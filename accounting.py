#!/usr/bin/env python
import urllib2 as ul
import sqlite3 as ps
from sqlite3 import OperationalError
from datetime import datetime
import sys

PRINT_STATS_AT_END = False

LOCALNET = '192.168.1.'
MIKROTIK = LOCALNET+'1'

dl = None
dc = None
dr = None

def update_host(ha,hb,bi,pi,year,month,day,hour,minute,second):
    hs = get_host(ha,hb)
    b = int(bi)+hs['bytes']
    p = int(pi)+hs['packets']
    dc.execute("UPDATE accounting_totals SET b="+str(b)+" WHERE ha='"+ha+"' AND hb='"+hb+"'")
    dc.execute("UPDATE accounting_totals SET p="+str(p)+" WHERE ha='"+ha+"' AND hb='"+hb+"'")
    dc.execute('''INSERT INTO accounting_history VALUES
                    (
                        '''+"'"+ha+"'"+''',
                        '''+"'"+hb+"'"+''',
                        '''+"'"+str(bi)+"'"+''',
                        '''+"'"+str(pi)+"'"+''',
                        '''+str(year)+''',
                        '''+str(month)+''',
                        '''+str(day)+''',
                        '''+str(hour)+''',
                        '''+str(minute)+''',
                        '''+str(second)+'''
                    )''')

def get_host(ha,hb):
    fr = dc.execute("SELECT * FROM accounting_totals WHERE ha='"+ha+"' AND hb='"+hb+"'").fetchall()
    if len(fr) == 0:
        dc.execute("INSERT INTO accounting_totals VALUES ('"+ha+"','"+hb+"',0,0)")
        fr = dc.execute("SELECT * FROM accounting_totals WHERE ha='"+ha+"' AND hb='"+hb+"'").fetchall()
    return {'bytes': int(fr[0][2]), 'packets': int(fr[0][3])}

# Display help if user requests it
bInitArg = True
for arg in sys.argv:
    if bInitArg:
        bInitArg = False
    else:
        if arg=="-H" or arg=="--help":
            print "MikroTik RouterOS bandwidth accounting toolbox by Quinn Ebert"
            print ""
            print "USAGE:"
            print sys.argv[0]+" [--help|-H] [--readonly|-R] [--stats-lifetime|-A] [-D=<file>]"
            print ""
            print "            --help|-H   shows this help message and exits"
            print "        --readonly|-R   don't gather/reset latest accounting snapshot"
            print "  --stats-lifetime|-A   show A=>B lifetime byte/packet totals (forces -R)"
            print "            -D=<file>   use <file> for database, not the built-in default"
            print ""
            print "Without any arguments, the script talks to the MikroTik, gathers the latest"
            print "snapshot and updates the database as relevant."
            print ""
            print "If the -D=<file> argument isn't used, the default database file behaviour"
            print "is to create or open accounting.sqlite3 in the current working directory."
            sys.exit()
# Process any other command line args
READONLY_MODE = False
DATABASE_FILE = "accounting.sqlite3"
bInitArg = True
for arg in sys.argv:
    if bInitArg:
        bInitArg = False
    else:
        if arg=="-R" or arg=="--readonly":
            READONLY_MODE = True
        elif arg=="-A" or arg=="--stats-lifetime":
            READONLY_MODE = True
            PRINT_STATS_AT_END = True
        elif arg.startswith("-D="):
            DATABASE_FILE = arg.split("=")[1]

dl = ps.connect(DATABASE_FILE,isolation_level=None)
dc = dl.cursor()
try:
    dr = dc.execute("SELECT * FROM accounting_totals")
except OperationalError:
    dc.execute("CREATE TABLE accounting_totals (ha text, hb text, b real, p real)")
try:
    dr = dc.execute("SELECT * FROM accounting_history")
except OperationalError:
    dc.execute('''CREATE TABLE accounting_history
                    (
                        ha text,
                        hb text,
                        b real,
                        p real,
                        snap_year real,
                        snap_month real,
                        snap_day real,
                        snap_hour real,
                        snap_minute real,
                        snap_second real
                    )''')

if not READONLY_MODE:
    res = ul.urlopen(ul.Request('http://'+MIKROTIK+'/accounting/ip.cgi')).read().rstrip().split("\n")
    pds = {}
    # Massage the data into a more useful format
    for rec in res:
        col = rec.split(" ")
        host_a = col[0]
        host_b = col[1]
        n_Byts = col[2]
        n_Pkts = col[3]
        if not host_a in pds:
            pds[host_a] = {}
            pds[host_a][host_b] = {}
            pds[host_a][host_b]['bytes'] = int(n_Byts)
            pds[host_a][host_b]['packets'] = int(n_Pkts)
        else:
            if not host_b in pds[host_a]:
                pds[host_a][host_b] = {}
                pds[host_a][host_b]['bytes'] = int(n_Byts)
                pds[host_a][host_b]['packets'] = int(n_Pkts)
            else:
                pds[host_a][host_b]['bytes'] += int(n_Byts)
                pds[host_a][host_b]['packets'] += int(n_Pkts)
    # Update DB
    for host_a,ignoreMe in pds.iteritems():
        for host_b,statInfo in pds[host_a].iteritems():
            update_host(host_a,host_b,statInfo['bytes'],statInfo['packets'],datetime.now().year,datetime.now().month,datetime.now().day,datetime.now().hour,datetime.now().minute,datetime.now().second)
else:
    print "[Read Only Mode Active]\n"

# Print statistics from DB
if PRINT_STATS_AT_END:
    fr = dc.execute("SELECT * FROM accounting_totals WHERE ha LIKE '"+LOCALNET+"%' OR hb LIKE '"+LOCALNET+"%' ORDER BY b ASC").fetchall()
    for fr_r in fr:
        (a,b,c,d) = fr_r
        e = float( float(c) / int(1024) / int(1024) )
        print str(a)+" => "+str(b)+"\n  "+str(c)+" b / "+str(round(e,3))+" MB ("+str(d)+" p)"
        if a.startswith(LOCALNET) and b.startswith(LOCALNET):
            print "    (WARNING: WAN passover traffic!)"
        elif b.startswith(LOCALNET) and not a.startswith(LOCALNET):
            print "    (WAN downstream)"
        elif a.startswith(LOCALNET) and not b.startswith(LOCALNET):
            print "    (WAN upstream)"
        else:
            print "    (WARNING: WAN scatter traffic!)"
