import psycopg2
import os ## for enviornment variable
import sys

HOME_PATH = os.environ['HOME']
# database info
LOCAL_DB = "host=%s/postgres dbname=postgres port=5432" %(HOME_PATH)

def query_read(cur, SQL):
    cur.execute(SQL)
    rows = cur.fetchall();
    for row in rows:
      for col in row:
        if( col is not None): # handle column with NULL 
          sys.stdout.write(str(col) + "\t") 
      print

def query_aggregator(cur, SQL):
  cur.execute(SQL)
  rows = cur.fetchall();
  return rows[0][0]

def question_3a(cur):
  print "Problem 3a"
  sql =  '''SELECT COUNT(*) 
            FROM (SELECT houseid, personid 
                  FROM nhts_day 
                  GROUP BY houseid,personid) AS foo;
          '''
  persons =  query_aggregator(cur, sql)
  
  for i in range(5, 105, 5):
    sql = '''SELECT COUNT(*) 
             FROM (SELECT houseid, personid 
                   FROM (SELECT houseid,personid,tdtrpnum,trpmiles 
                         FROM nhts_day WHERE trpmiles > 0) AS pos 
                   GROUP BY pos.houseid, pos.personid 
                   HAVING SUM(pos.trpmiles) < %s) AS foo ;
          ''' %(i) 
    counts =  query_aggregator(cur, sql)
    print "%f (%d/%d) of persons travel less than %d miles a day" %(counts/float(persons), counts, persons, i) 

def question_3b(cur):
  print "Problem 3b"
  for i in range(5,105,5):
    sql = '''
          SELECT AVG(epatmpg)
          FROM
            ( SELECT houseid, vehid 
              FROM nhts_day 
              WHERE trpmiles > 0 AND trpmiles < %d 
                   AND cast(vehid as SMALLINT) > 0
            ) AS A 
            NATURAL JOIN
            ( SELECT houseid, vehid, epatmpg
              FROM nhts_veh
            ) AS B
          ;
          ''' %(i)
    print "<%d miles, %f" %(i, query_aggregator(cur, sql))

def _question_3c(cur, debug):
  if(debug): 
    print "Problem 3c"
  sql = '''
        SELECT value, yyyymm
        FROM EIA_TRANS 
        WHERE MSN = 'TEACEUS' 
              AND YYYYMM BETWEEN 200803 AND 200904 
              AND CAST(YYYYMM as CHAR(6)) NOT LIKE '%13'
        ORDER BY yyyymm ;
  ''' ## total co2 emission from transportation for each date from 200803-200904
  cur.execute(sql)
  eia_rows = cur.fetchall() 
 
  sql =  '''
         SELECT COUNT(houseid)
         FROM   nhts_hh
         GROUP BY tdaydate
         ORDER BY tdaydate;
         ''' 
         ## counts the  number of households per month in the survey

  cur.execute(sql)
  hh_nums = cur.fetchall()
  scale_ups = ( (117538000 / row[0]) for row in hh_nums ) # 117538000 households in the U.S.
  days = [31,30,31,30,31,30,31,30,31,30,31,28,31,30] # enumerating days of month from 200803 - 200904
 
  sql = '''
        SELECT SUM(trpmiles / epatmpg * 0.000000008887)
        FROM ( SELECT trpmiles, houseid, vehid, tdaydate
               FROM nhts_day ) AS A
             NATURAL JOIN 
             ( SELECT epatmpg , houseid, vehid, tdaydate
               FROM nhts_veh ) AS B
        WHERE trpmiles > 0 AND epatmpg > 0 
        GROUP BY tdaydate
        ORDER BY tdaydate;
        '''	
  ## look for CO2 emission consumed for households vehicle for each month 
  ## CO2 emission for a vehicle = 
  ## vmt_mile [miles] / eiadmpg [miles/gal] * 0.000000008887 [millon metric tons of CO2 emission / gal]]

  cur.execute(sql)
  nhts_rows = cur.fetchall() 
  rtn = list() 

  for day, scale_up, rhts_row, eia_row in zip(days, scale_ups, nhts_rows, eia_rows):
      if(debug):
        print "Household vehicles in month: %d attributes %0.2f%% of total transportation CO2 emission" \
           %(eia_row[1], (day * scale_up * rhts_row[0] / eia_row[0] * 100) )
      rtn.append( day*scale_up*rhts_row[0] ) 
  
  return rtn

def question_3c(cur):
  _question_3c(cur, True)

def question_3d(cur):
  print "Problem 3d"

  pureFuels  = _question_3c(cur, False)

# for miles per day(mpd) <= 20, mpd is a derived variable
# epatmpg * 0.090634441 = [miles/kWh] -- energy efficiency
# mpd / [miles/kWh] = [kWh] consumed per day
# ratio = million metric tons CO2 from eia_elect / million kWh from eia_mkwh
# kwH * ratio = [million metric tons CO2] per day -- still need to * by days of month

# for trpmiles > 20
# use miles per day (mpd) - 20, then repeat some procesudre from 3c  

  days = [31,30,31,30,31,30,31,30,31,30,31,28,31,30] # enumerating days of month from 200803 - 200904
  
  for i in [20, 40, 60]:
    print "Plug-in hybrids with %d mile electric range:" %(i)
    # CO2 emission just from the first X miles using electricity
    sql = ''' 
      SELECT (A.kWh * B.e2c * C.scale /1000000) AS CO2, A.tdaydate
      FROM
       (SELECT SUM( mpd / ( epatmpg * 0.090634441) ) AS kWh, tdaydate
        FROM
          ( SELECT houseid, vehid, tdaydate, sum(trpmiles) AS mpd
            FROM nhts_day
            GROUP BY houseid, vehid, tdaydate
            HAVING sum(trpmiles) <= %d 
            ORDER BY tdaydate
          ) AS A
          NATURAL JOIN
          ( 
            SELECT houseid, vehid, tdaydate, epatmpg
            FROM nhts_veh
          ) AS B
        GROUP BY tdaydate
        ORDER BY tdaydate

       ) AS A  -- kWh is for a travel day, need to * days of month
       ,  
       (SELECT eia_elec.value/eia_mkwh.value AS e2c, eia_elec.YYYYMM 
        FROM eia_elec, eia_mkwh
        WHERE eia_elec.msn = 'TXEIEUS' AND eia_mkwh.msn = 'ELETPUS'
              AND eia_elec.YYYYMM = eia_mkwh.YYYYMM
              AND eia_elec.YYYYMM BETWEEN 200803 AND 200904
              AND CAST(eia_elec.YYYYMM AS CHAR(6)) NOT LIKE '%%13'
              AND CAST(eia_mkwh.YYYYMM AS CHAR(6)) NOT Like '%%13'
        ORDER BY eia_elec.YYYYMM

       ) AS B   --- e2c is the ratio converting from kwh to metric ton of CO2
       ,
       (SELECT (117538000 / COUNT(houseid)) AS scale, tdaydate
        FROM   nhts_hh
        GROUP BY tdaydate
        ORDER BY tdaydate

       ) AS C  -- scale is (total # of U.S. households ) / (# of households in the survey in a month) 
      WHERE  A.tdaydate = CAST(B.YYYYMM AS CHAR(6)) 
             AND A.tdaydate = C.tdaydate
             AND CAST(B.YYYYMM AS CHAR(6)) = C.tdaydate
      ;
      ''' %(i) 
    cur.execute(sql)
    elec_rows = cur.fetchall()

    sql = '''
    SELECT A.CO2 * B.scale
    FROM
      (SELECT SUM( (mpd - %d) / epatmpg * 0.000000008887) AS CO2 ,tdaydate
      FROM
        ( SELECT houseid, vehid, tdaydate, sum(trpmiles) AS mpd
          FROM nhts_day
          GROUP BY houseid, vehid, tdaydate
          HAVING sum(trpmiles) > %d 
          ORDER BY tdaydate
        ) AS A
        NATURAL JOIN
        ( 
          SELECT houseid, vehid, tdaydate, epatmpg
          FROM nhts_veh
        ) AS B
      GROUP BY tdaydate
      ORDER BY tdaydate
     ) AS A
     ,
     (SELECT (117538000 / COUNT(houseid)) AS scale, tdaydate
      FROM   nhts_hh
      GROUP BY tdaydate
      ORDER BY tdaydate
     ) AS B
    WHERE A.tdaydate = B.tdaydate
    ;
    ''' %(i, i)
    cur.execute(sql)
    fuel_rows = cur.fetchall()
    for elec_row, fuel_row, day, pureFuel in zip(elec_rows, fuel_rows, days, pureFuels):
      delta =  pureFuel - (elec_row[0] + fuel_row[0]) * day 
      delta_percentage = delta / pureFuel * 100
      month =  elec_row[1]
      print "The difference in CO2 emission (million metric tons) is %f in %s| a %.2f%% change" %(delta, month, delta_percentage)
    print "-----------------------------"




def question_5a(cur):
  print "Problem 5a"

  pureFuels  = _question_3c(cur, False)

# for miles per day(mpd) <= 20, mpd is a derived variable
# epatmpg * 0.090634441 = [miles/kWh] -- energy efficiency
# mpd / [miles/kWh] = [kWh] consumed per day
# ratio = million metric tons CO2 from eia_elect / million kWh from eia_mkwh
# kwH * ratio = [million metric tons CO2] per day -- still need to * by days of month

# for trpmiles > 20
# use miles per day (mpd) , then repeat some procesudre from 3c  

  days = [31,30,31,30,31,30,31,30,31,30,31,28,31,30] # enumerating days of month from 200803 - 200904
  
  for i in [84, 107, 208, 270]:
    print "%d miles threshold :" %(i)
    # CO2 emission just from the first X miles using electricity
    sql = ''' 
      SELECT (A.kWh * B.e2c * C.scale /1000000) AS CO2, A.tdaydate
      FROM
       (SELECT SUM( mpd / ( epatmpg * 0.090634441) ) AS kWh, tdaydate
        FROM
          ( SELECT houseid, vehid, tdaydate, sum(trpmiles) AS mpd
            FROM nhts_day
            GROUP BY houseid, vehid, tdaydate
            HAVING sum(trpmiles) <= %d 
            ORDER BY tdaydate
          ) AS A
          NATURAL JOIN
          ( 
            SELECT houseid, vehid, tdaydate, epatmpg
            FROM nhts_veh
          ) AS B
        GROUP BY tdaydate
        ORDER BY tdaydate

       ) AS A  -- kWh is for a travel day, need to * days of month
       ,  
       (SELECT eia_elec.value/eia_mkwh.value AS e2c, eia_elec.YYYYMM 
        FROM eia_elec, eia_mkwh
        WHERE eia_elec.msn = 'TXEIEUS' AND eia_mkwh.msn = 'ELETPUS'
              AND eia_elec.YYYYMM = eia_mkwh.YYYYMM
              AND eia_elec.YYYYMM BETWEEN 200803 AND 200904
              AND CAST(eia_elec.YYYYMM AS CHAR(6)) NOT LIKE '%%13'
              AND CAST(eia_mkwh.YYYYMM AS CHAR(6)) NOT Like '%%13'
        ORDER BY eia_elec.YYYYMM

       ) AS B   --- e2c is the ratio converting from kwh to metric ton of CO2
       ,
       (SELECT (117538000 / COUNT(houseid)) AS scale, tdaydate
        FROM   nhts_hh
        GROUP BY tdaydate
        ORDER BY tdaydate

       ) AS C  -- scale is (total # of U.S. households ) / (# of households in the survey in a month) 
      WHERE  A.tdaydate = CAST(B.YYYYMM AS CHAR(6)) 
             AND A.tdaydate = C.tdaydate
             AND CAST(B.YYYYMM AS CHAR(6)) = C.tdaydate
      ;
      ''' %(i) 
    cur.execute(sql)
    elec_rows = cur.fetchall()

    sql = '''
    SELECT A.CO2 * B.scale
    FROM
      (SELECT SUM( mpd / epatmpg * 0.000000008887) AS CO2 ,tdaydate
      FROM
        ( SELECT houseid, vehid, tdaydate, sum(trpmiles) AS mpd
          FROM nhts_day
          GROUP BY houseid, vehid, tdaydate
          HAVING sum(trpmiles) > %d 
          ORDER BY tdaydate
        ) AS A
        NATURAL JOIN
        ( 
          SELECT houseid, vehid, tdaydate, epatmpg
          FROM nhts_veh
        ) AS B
      GROUP BY tdaydate
      ORDER BY tdaydate
     ) AS A
     ,
     (SELECT (117538000 / COUNT(houseid)) AS scale, tdaydate
      FROM   nhts_hh
      GROUP BY tdaydate
      ORDER BY tdaydate
     ) AS B
    WHERE A.tdaydate = B.tdaydate
    ;
    ''' %(i)
    cur.execute(sql)
    fuel_rows = cur.fetchall()
    for elec_row, fuel_row, day, pureFuel in zip(elec_rows, fuel_rows, days, pureFuels):
      delta =  pureFuel - (elec_row[0] + fuel_row[0]) * day 
      delta_percentage = delta / pureFuel * 100
      month =  elec_row[1]
      print "The difference in CO2 emission (million metric tons) is %f in %s| a %.2f%% change" %(delta, month, delta_percentage)
    print "-----------------------------"

def question_5b(cur):
  print "Problem 5b"

  pureFuels  = _question_3c(cur, False)

# for miles per day(mpd) <= 20, mpd is a derived variable
# epatmpg * 0.090634441 = [miles/kWh] -- energy efficiency
# mpd / [miles/kWh] = [kWh] consumed per day
# ratio = million metric tons CO2 from eia_elect / million kWh from eia_mkwh
# kwH * ratio = [million metric tons CO2] per day -- still need to * by days of month

# for trpmiles > 20
# use miles per day (mpd) , then repeat some procesudre from 3c  

  days = [31,30,31,30,31,30,31,30,31,30,31,28,31,30] # enumerating days of month from 200803 - 200904
  
  for i in [84, 107, 208, 270]:
    print "%d miles threshold :" %(i)
    # CO2 emission just from the first X miles using electricity
    sql = ''' 
      SELECT (A.kWh * B.e2c * C.scale /1000000) AS CO2, A.tdaydate
      FROM
       (SELECT SUM( mpd / ( epatmpg * 0.090634441) ) AS kWh, tdaydate
        FROM
          ( SELECT houseid, vehid, tdaydate, sum(trpmiles) AS mpd
            FROM nhts_day
            GROUP BY houseid, vehid, tdaydate
            HAVING sum(trpmiles) <= %d 
            ORDER BY tdaydate
          ) AS A
          NATURAL JOIN
          ( 
            SELECT houseid, vehid, tdaydate, epatmpg
            FROM nhts_veh
          ) AS B
        GROUP BY tdaydate
        ORDER BY tdaydate

       ) AS A  -- kWh is for a travel day, need to * days of month
       ,  
       (SELECT (B.co2 / A.mkwh) AS e2c, A.YYYYMM AS YYYYMM
        FROM   ( SELECT (A1.value + A2.value + A3.value) AS mkwh, A1.YYYYMM
                 FROM eia_mkwh AS A1, eia_mkwh AS A2, eia_mkwh AS A3
                 WHERE A1.msn = 'NUETPUS' AND CAST(A1.YYYYMM AS CHAR(6)) LIKE '2014%%' AND
                    A2.msn = 'NGETPUS' AND A1.YYYYMM = A2.YYYYMM AND
                    A3.msn = 'WYETPUS' AND A1.YYYYMM = A3.YYYYMM AND
                    A2.YYYYMM = A3.YYYYMM AND CAST(A1.YYYYMM AS CHAR(6)) NOT LIKE '201413'
               ) AS A
               ,
               ( SELECT value AS co2, YYYYMM   
                 FROM eia_elec
                 WHERE CAST(YYYYMM AS CHAR(6)) LIKE '2014%%' AND
                    CAST(YYYYMM AS CHAR(6)) NOT LIKE '201413' AND
                    msn = 'NNEIEUS'
               ) AS B
        WHERE A.YYYYMM = B.YYYYMM
        ORDER BY YYYYMM
       ) AS B   --- e2c is the ratio converting from kwh to metric ton of CO2
       ,
       (SELECT (117538000 / COUNT(houseid)) AS scale, tdaydate
        FROM   nhts_hh
        GROUP BY tdaydate
        ORDER BY tdaydate

       ) AS C  -- scale is (total # of U.S. households ) / (# of households in the survey in a month) 
      WHERE  CAST(A.tdaydate AS INT) + 598 = B.YYYYMM
             AND A.tdaydate = C.tdaydate
             AND B.YYYYMM  = CAST(C.tdaydate AS INT) + 598
      ;
      ''' %(i) 
    cur.execute(sql)
    elec_rows = cur.fetchall()

    sql = '''
    SELECT A.CO2 * B.scale
    FROM
      (SELECT SUM( mpd / epatmpg * 0.000000008887) AS CO2 ,tdaydate
      FROM
        ( SELECT houseid, vehid, tdaydate, sum(trpmiles) AS mpd
          FROM nhts_day
          GROUP BY houseid, vehid, tdaydate
          HAVING sum(trpmiles) > %d 
          ORDER BY tdaydate
        ) AS A
        NATURAL JOIN
        ( 
          SELECT houseid, vehid, tdaydate, epatmpg
          FROM nhts_veh
        ) AS B
      GROUP BY tdaydate
      ORDER BY tdaydate
     ) AS A
     ,
     (SELECT (117538000 / COUNT(houseid)) AS scale, tdaydate
      FROM   nhts_hh
      GROUP BY tdaydate
      ORDER BY tdaydate
     ) AS B
    WHERE A.tdaydate = B.tdaydate
    ;
    ''' %(i)
    cur.execute(sql)
    fuel_rows = cur.fetchall()
    for elec_row, fuel_row, day, pureFuel in zip(elec_rows, fuel_rows, days, pureFuels):
      delta =  pureFuel - (elec_row[0] + fuel_row[0]) * day 
      delta_percentage = delta / pureFuel * 100
      month =  elec_row[1]
      print "The difference in CO2 emission (million metric tons) is %f in %s| a %.2f%% change" %(delta, month, delta_percentage)
    print "-----------------------------"





def main():
  conn = psycopg2.connect(LOCAL_DB)
  cur = conn.cursor()
  
  print "Queries for Problem 3"
  ans=True
  while ans != 0:
    print("""
    Press 1 for problem 3a
    Press 2 for problem 3b
    Press 3 for problem 3c
    Press 4 for problem 3d
    Press 5 for problem 5a
    Press 6 for problem 5b
    Press 7 to quit
    """)
    ans=raw_input("Which query would you like to run? ")
    if ans=="1":
      question_3a(cur)
    elif ans=="2":
      question_3b(cur)
    elif ans=="3":
      question_3c(cur)
    elif ans=="4":
      question_3d(cur)
    elif ans=="5":
      question_5a(cur)
    elif ans=="6":
      question_5b(cur)
    elif ans=="7":
	  ans=0
    else:
      print("\nInvalid selection") 


  conn.commit()
  conn.close()

main()
