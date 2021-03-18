"""Query over CouchBase bucket , make a dataset  and check the integrity where timestamp respect time range """
__author__ = "Andrei Lazar"
__status__ = 'Prototype'

#import lib
import os
import sys

from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
import pandas as pd

import script_config

def integityChecker (df_toverify,timeset ,maxtolerance,name ,show):
    ###########CHECKER INTEGRITY##############################
    d2 = 0
    incoerencefind = 0
    less = 0
    underminute = 0
    over = 0
    totalsearch = 0
    print("\n############################################")
    print("#####          CHECK INTEGRITY       #######")
    print(f"#####           {name}               #######")
    print("############################################")
    for index, row in df_toverify.iterrows():
        d1 = pd.to_datetime(index)

        if d2 == 0:
            d2 = d1
        elif (d1 - d2).total_seconds() > timeset:
            result = (d1 - d2).total_seconds()
            # if you want see row  where not respect range
            if show == True :
               print(f"The difference betwen befor and after is {result}second ,\n"
                     f"row before:{d2} and row after: {d1}\n")
            if result <= maxtolerance:
                less += 1
            elif result >= 60:
                over += 1
                print(f"The difference between befor and after is {result}second ,\n"
                      f"row before:{d2} and row after: {d1}\n")
            else:
                underminute += 1
                #Uncomment if you want show
                '''print(f"The difference between befor and after is {result}second ,\n"
                      f"row before:{d2} and row after: {d1}\n") '''
            d2 = d1
            incoerencefind += 1
        else:
            d2 = d1
        totalsearch += 1
    if incoerencefind != 0 :
        print(f">>>> I find {incoerencefind} time spaces than {less} less or equal {maxtolerance} seconds , {underminute} over {maxtolerance} seconds but under minute, consistent gaps are {over} over minute  in dataset of: {totalsearch} rows")
    else:
        print("ALL PASS!")


if __name__ == '__main__':

    time_condition = script_config.querry_condition
    user = script_config.user


    pathname = os.path.dirname(sys.argv[0])

    # dictionary to save the mac
    dic_mac = {"F8:85:CC:71:7D:A2": "Beacon 1", "C1:15:DF:9D:D9:2B": "Beacon 2", "CF:9C:46:06:15:64": "Beacon 3",
               "D6:6E:1F:C0:B9:17": "Beacon 4"}

    # get a reference to our cluster
    cluster = Cluster(script_config.cluster_path, ClusterOptions(
        PasswordAuthenticator(script_config.cb_user, script_config.cb_password)))

    # get a reference to our bucket
    cb = cluster.bucket(script_config.bucket_name)
    cb.n1ql_timeout = 5600
    cb.timeout = 5600
    cluster.n1ql_timeout = 5600
    cluster.timeout = 5600
    try:
      #print(" ###############---Beacon Table--################")
      query_string = f"select data from staging where contains(`savedOn`, '{time_condition}') and `user`=$user and hand='sx' and type='beacon'"
      row_iter = cluster.query(query_string, user=user)
      df_beacon = pd.DataFrame()
      temp_beacon = pd.DataFrame()

      #Query over bucket
      for row in row_iter:
          temp_beacon = pd.DataFrame.from_dict(row['data'], orient='index')
          df_beacon = df_beacon.append(temp_beacon, ignore_index=False)

      #Make a index timestamp object and sort by time
      df_beacon.index = pd.to_datetime(df_beacon.index)
      df_beacon.sort_index(inplace=True)

      #print(df_beacon.to_markdown())

      #Verify coerence dataset
      integityChecker(df_beacon,1.2,3,'BEACON',False)

      ####DATAFRAME TO CSV ###########
      df_beacon.to_csv(fr'{pathname}/beacon_dataframe_{time_condition}.csv',index=True,header=True)

      #Create another dataframe to improve legibility
      df_beacon_result = pd.DataFrame()

      df_beacon_result['#mac'] = df_beacon.idxmax(axis=1).map(dic_mac)
      df_beacon_result['#value'] = df_beacon.max(axis=1)

      #print legibile dataset
      #print(df_beacon_result.head(20))
    except Exception as e:
        print("Error during Beacon processing:", e)

    try:
      #print(" ###############---Sensor Table--################")
      #Query string
      query_string = f"select data from staging where contains(`savedOn`, '{time_condition}') and `user`=$user and hand='sx' and type='sensor'"
      row_iter = cluster.query(query_string, user=user)
      df_sensor = pd.DataFrame()
      temp_sensor = pd.DataFrame()

      # Query over bucket
      for row in row_iter:
          temp_sensor = pd.DataFrame.from_dict(row['data'], orient='index')
          df_sensor = df_sensor.append(temp_sensor, ignore_index=False)

      # Make a index timestamp object and sort by time
      df_sensor.index = pd.to_datetime(df_sensor.index)
      df_sensor.sort_index(inplace=True)


      #print(df_sensor.to_markdown())

      #this is to fill the nan using the previous data
      df_sensor_fill = df_sensor.fillna(method='ffill')

      #print(df_sensor_fill.to_markdown())
      #print(df_sensor_fill.head(20))

      #Integrity check
      # Verify coerence dataset
      integityChecker(df_sensor, 0.4, 0.6, 'SENSOR',False)

      ####DATAFRAME TO CSV ###########
      df_sensor_fill.to_csv(fr'{pathname}/sensor_dataframe_{time_condition}.csv', index=True, header=True)

    except Exception as e:
        print("Error during Sensor processing:", e)

    try:
      #print("######Merge")
      result_df = pd.merge_asof(
          df_sensor_fill,
          df_beacon_result,
          # on="time",
          # by="ticker",
          tolerance=pd.Timedelta("400ms"),
          allow_exact_matches=False,
          left_index=True,
          right_index=True
      )
      # result_df = pd.concat([df_b_result,temp_df],axis=1)
      # result_df = df_b_result.join(temp_df, how="outer")

      #print(result_df.to_markdown())
      #print(len(result_df.index))
      # print(result_df.head(100))

      result_df_final = result_df.fillna(method='ffill')
      # print(result_df_final.head(10))
    except Exception as e: print("Error during Merge processing:", e)