import json

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import lit
from pyspark.sql import SQLContext,SparkSession
import sys

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col,array_contains,struct,when
from pyspark.sql.types import *

####################################################################################################################
########################################### Spark Initialisation ###################################################
####################################################################################################################

conf = SparkConf()
conf.setAppName("FPL")
sc = SparkContext(conf=conf)
sqlContext = SparkSession.builder.getOrCreate()
ssc = StreamingContext(sc, 5)
ssc.checkpoint("checkpoint_FPL")

players_df = sqlContext.read.load("/user/arshgoyal/csv/players.csv", format="csv", header="true", inferSchema="true")
teams_df = sqlContext.read.load("/user/arshgoyal//csv/teams.csv", format="csv", header="true", inferSchema="true")

# print(players_df)
# name='Chris Gunter'
# row=players_df.filter(players_df.name==name)
# role=row.collect()[0].role
# print(role)

def check_valid(team_positions):
  if team_positions['GK']!=1:
    return False
  if team_positions['DF']<3:
    return False
  if team_positions['MD']<2:
    return False
  if team_positions['FW']<1:
    return False

  return True

def predict_winner():

    with open('/home/arshgoyal/Desktop/BD_Proj/json/inp_predict.json') as f:
        input_data = json.load(f)
    # query = sqlContext.read.json("/user/arshgoyal/json/inp_predict.json", multiLine = True)
    # query.show()
    # print(input_data.keys())


    team1=input_data['team1']
    team2=input_data['team2']
    team1_name=team1['name']
    team2_name=team2['name']

    team1_positions={'GK':0,'DF':0,'MD':0,'FW':0}
    team2_positions={'GK':0,'DF':0,'MD':0,'FW':0}

    for i in range(1,12):
        name=team1['player'+str(i)]
        row = players_df.filter(players_df.name == name)
        role = row.collect()[0].role
        if role not in team1_positions:
            team1_positions[role]=1
        else:
            team1_positions[role]+=1

    # team2_positions={}
    for i in range(1,12):
        name=team2['player'+str(i)]
        row = players_df.filter(players_df.name == name)
        role = row.collect()[0].role
        if role not in team2_positions:
            team2_positions[role]=1
        else:
            team2_positions[role]+=1

    if not check_valid(team1_positions) or not check_valid(team2_positions):
        print('Invalid')
        return

    else:
    # regression check for player rating.

        with open('/home/arshgoyal/Desktop/BD_Proj/chemistry.json') as json_file:
            players_combination = json.load(json_file)

        player_stats = sqlContext.read.json('/user/arshgoyal/Player_Rating.json/part-00000-19bbeedf-319d-4aec-b575-15336401675f-c000.json')

        team1_player_ids=[]
        team2_player_ids=[]

        for i in range(1,12):
            name1=team1['player'+str(i)]
            row = players_df.filter(players_df.name == name1)
            player_id = row.collect()[0].Id
            team1_player_ids.append(player_id)

            name2=team2['player'+str(i)]
            row = players_df.filter(players_df.name == name2)
            player_id = row.collect()[0].Id
            team2_player_ids.append(player_id)

        # print(team1_player_ids)

        team1_player_chemistries={}
        team2_player_chemistries={}
        for i in range(1,12):
            team1_player_chemistries[team1_player_ids[i-1]]=[]
            team2_player_chemistries[team2_player_ids[i-1]]=[]

        for i in range(11):
            for j in range(i+1,11):
                if str(team1_player_ids[i])+' '+str(team1_player_ids[j]) in players_combination:
                    team1_player_chemistries[team1_player_ids[i]].append(float(players_combination[str(team1_player_ids[i])+' '+str(team1_player_ids[j])]))
                    team1_player_chemistries[team1_player_ids[j]].append(float(players_combination[str(team1_player_ids[i]) + ' ' + str(team1_player_ids[j])]))

                else:
                    team1_player_chemistries[team1_player_ids[i]].append(float(players_combination[str(team1_player_ids[j])+' '+str(team1_player_ids[i])]))
                    team1_player_chemistries[team1_player_ids[j]].append(float(players_combination[str(team1_player_ids[j]) + ' ' + str(team1_player_ids[i])]))


                if str(team2_player_ids[i])+' '+str(team2_player_ids[j]) in players_combination:
                    team2_player_chemistries[team2_player_ids[i]].append(float(players_combination[str(team2_player_ids[i])+' '+str(team2_player_ids[j])]))
                    team2_player_chemistries[team2_player_ids[j]].append(float(players_combination[str(team2_player_ids[i]) + ' ' + str(team2_player_ids[j])]))
                else:
                    team2_player_chemistries[team2_player_ids[i]].append(float(players_combination[str(team2_player_ids[j])+' '+str(team2_player_ids[i])]))
                    team2_player_chemistries[team2_player_ids[j]].append(float(players_combination[str(team2_player_ids[j]) + ' ' + str(team2_player_ids[i])]))

        # print(team1_player_chemistries)
        # team1_consolidated_chemistries={key:(sum(value)/10.0)*player_rating for key,value in team1_player_chemistries.items()}
        # team2_consolidated_chemistries={key:(sum(value)/10.0)*player_rating for key,value in team2_player_chemistries.items()}

        team1_consolidated_chemistries={key:(sum(value)/10.0) for key,value in team1_player_chemistries.items()}
        team2_consolidated_chemistries={key:(sum(value)/10.0) for key,value in team2_player_chemistries.items()}

        for i in player_stats.collect():
            if i.Id in team1_consolidated_chemistries:
                team1_consolidated_chemistries[i.Id]=team1_consolidated_chemistries[i.Id]*i.ratings._2
            elif i.Id in team2_consolidated_chemistries:
                team2_consolidated_chemistries[i.Id]=team2_consolidated_chemistries[i.Id]*i.ratings._2

        # for i in team1_player_ids:
        #   row=player_stats.filter(player_stats.Id==i)
        #   rating=row.collect()[0].ratings._2
        #   team1_consolidated_chemistries[i] = team1_consolidated_chemistries[i] * rating

        # for i in team2_player_ids:
        #   row = player_stats.filter(player_stats.Id == i)
        #   rating = row.collect()[0].ratings._2
        #   team2_consolidated_chemistries[i] = team2_consolidated_chemistries[i] * rating

        # team1_player_ratings={}
        # team2_player_ratings={}
        # for i in range(11):
        #   team1_player_ratings[team1_player_ids[i]]=player_stats['Id'][team1_player_ids[i]]
        #   team1_player_ratings['player_rating']=player_stats['ratings']['_2']
        #   team2_player_ratings['id'] = player_stats['Id']
        #   team2_player_ratings['player_rating'] = player_stats['ratings']['_2']

        # print(team1_consolidated_chemistries)

        strength_team1=sum(list(team1_consolidated_chemistries.values()))/11.0
        # print(strength_team1)
        strength_team2=sum(list(team2_consolidated_chemistries.values()))/11.0
        # print(strength_team2)

        winning_chance_team1=(0.5+(strength_team1)-((strength_team1+strength_team2)/2.0))*100
        winning_chance_team2=(0.5+(strength_team2)-((strength_team2+strength_team1)/2.0))*100
        # print(winning_chance_team1)

        out_predict={}
        out_predict['team1']={"name":team1_name,"winning chance":winning_chance_team1}
        out_predict['team2']={"name":team2_name,"winning chance":winning_chance_team2}
        print(out_predict)
        print('##############################################################################')
        with open("/home/arshgoyal/Desktop/BD_Proj/out_predict.json", "w") as outfile:  
            json.dump(out_predict, outfile) 


        '''
        l = []
        schema = StructType()
        df = sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD(),schema)
        #l = [[{"name":team1_name,"winning chance":winning_chance_team1}]]
        dic = {"name":team1_name,"winning chance":winning_chance_team1}
        l.append(dic)
        #print(l)
        l = str(l)
        print(l)
        df = df.withColumn("team1",lit(l))
        #temp = sqlContext.createDataFrame(l)
        #df = df.union(temp)
        df.show()
        '''
        #new_df.write.json("file:///home/arshgoyal/Desktop/BD_Proj/out_player.json","overwrite")


def player_profile():
    query = sqlContext.read.json("/user/arshgoyal/json/inp_player.json", multiLine = True)
    name = query.collect()[0].name

    new_df=players_df.filter(players_df.name == name)
    #new_df.show()

    player_Id = new_df.collect()[0].Id
    #print(player_Id)

    #player_Profile = sqlContext.read.json("file:///home/arshgoyal/Desktop/BD_Proj/profile.json")
    player_Profile = sqlContext.read.json("/user/arshgoyal/Player_Profile.json/part-00000-38ac3af5-f55b-4042-87dc-8ea7fcc5e290-c000.json")
    #player_Profile.show()

    player_Profile = player_Profile.filter(player_Profile.Id == player_Id)
    player_Profile.show()

    metrics = player_Profile.collect()[0].metrics
    fouls = metrics._2
    goals = metrics._3
    own_goals = metrics._4
    pass_accuracy = metrics._5
    shots_target = metrics._6
    print(metrics)
    print("dddddddddddddddddddddddddddddddddddddd")

    new_df = new_df.withColumn("fouls",lit(fouls))
    new_df = new_df.withColumn("goals",lit(goals))
    new_df = new_df.withColumn("own_goals",lit(own_goals))
    new_df = new_df.withColumn("percent_pass_accuracy",lit(pass_accuracy))
    new_df = new_df.withColumn("percent_shots_on_target",lit(shots_target))

    new_df.show()

    new_df.write.json("file:///home/arshgoyal/Desktop/BD_Proj/out_player.json","overwrite")

def match_details():
    query = sqlContext.read.json("/user/arshgoyal/json/inp_match_cust.json", multiLine = True)
    query.show()

    date = query.collect()[0].date
    label = query.collect()[0].label
    #teams = label[0]
    #score = label[1]
    print(date)
    print(label)
    #print(teams)
    #print(score)
    print("######################################")

    match_Details = sqlContext.read.json("/user/arshgoyal/Match_details.json/part-00000-ca4a45f2-b4b4-4f7d-8a05-bc4e0d639b9a-c000.json")
    match_Details.show()


    print(match_Details.count())
    print("#################################")
    #for i in range(len(match_Details)):
    details = ''
    for i in match_Details.collect():
        if (date == i.match_Id[0]):
        #print(i.match_Id[1])
        #print(label)
            if(label == i.match_Id[1]):
                details = i.details

    # print(details[0])

    '''
    Row(date='2017-08-12', duration='Regular', gameweek='1', 
    goals='[[214654, 1673, 2]]', 
    label='Crystal Palace - Huddersfield Town, 0 - 3', 
    own_goals='[[8221, 1628, 1]]', 
    red_cards='[]', 
    venue='Selhurst Park', 
    winner='1673', 
    yellow_cards='[8142, 11078, 279709, 214654]')
    '''

    #l1=[1,2,3]

    date = details[0].date
    duration = details[0].duration

    schema = StructType([StructField('date',StringType(),False)])
    new_df = sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD(),schema)
    l = [[date]]

    temp = sqlContext.createDataFrame(l)
    new_df = new_df.union(temp)


    new_df = new_df.withColumn("duration",lit(details[0].duration))
    new_df = new_df.withColumn("duration",lit(details[0].gameweek))

    if(details[0].winner != '0'):
        temp=teams_df.filter(teams_df.Id == details[0].winner)
        new_df = new_df.withColumn("winner",lit(temp.collect()[0].name))
    else:
        l = "NULL"
        new_df = new_df.withColumn("winner",lit(l))



    new_df = new_df.withColumn("gameweek",lit(details[0].gameweek))



    goals = details[0].goals
    goals=goals.replace('[','')
    goals=goals.replace(']','')
    goals = [goals.split(',')]
    #print(goals)
    #print("#############################")



    if(len(goals[0]) == 1):
        l = "NONE"
    else:
        l =[]
        for i in goals:
            temp=players_df.filter(players_df.Id == i[0])
            name = temp.collect()[0].name
            temp = teams_df.filter(teams_df.Id == i[1])
            team = temp.collect()[0].name
            goal = i[2]
            dic = {"name" : name , "team" : team , "number_of_goals" : goal} 
            l.append(dic)
        #print(l)
        l = str(l)

    new_df = new_df.withColumn("goals",lit(l))




    own_goals = details[0].own_goals
    own_goals=own_goals.replace('[','')
    own_goals=own_goals.replace(']','')
    own_goals = [own_goals.split(',')]
    print(len(own_goals[0]))
    print("#############################")


    if(len(own_goals[0]) == 1):
        l = "NONE"
    else:
        l =[]
        for i in own_goals:
            temp=players_df.filter(players_df.Id == i[0])
            name = temp.collect()[0].name
            temp = teams_df.filter(teams_df.Id == i[1])
            team = temp.collect()[0].name
            goal = i[2]
            dic = {"name" : name , "team" : team , "number_of_goals" : goal} 
            l.append(dic)
    #print(l)
        l = str(l)
    new_df = new_df.withColumn("own_goals",lit(l))


    yellow_cards = details[0].yellow_cards
    yellow_cards=yellow_cards.replace('[','')
    yellow_cards=yellow_cards.replace(']','')
    yellow_cards = yellow_cards.split(',')


    if(len(yellow_cards) == 1):
        l = "NONE"
    else:
        l = []
        for i in yellow_cards:
            temp=players_df.filter(players_df.Id == i)
            name = temp.collect()[0].name
            l.append(name)
        print(l)
        l = str(l)

    new_df = new_df.withColumn("yellow_cards",lit(l))




    red_cards = details[0].red_cards
    red_cards=red_cards.replace('[','')
    red_cards=red_cards.replace(']','')
    red_cards = red_cards.split(',')
    print(red_cards)
    print(len(red_cards))

    if(len(red_cards) == 1):
        l = "NONE"
    else:
        l = []
        for i in red_cards:
            temp=players_df.filter(players_df.Id == i)
            name = temp.collect()[0].name
            l.append(name)
        print(l)
        l = str(l)

    new_df = new_df.withColumn("red_cards",lit(l))


    new_df.show()


    new_df.write.json("file:///home/arshgoyal/Desktop/BD_Proj/out_match.json","overwrite")

query = sqlContext.read.json("/user/arshgoyal/json/inp_predict.json", multiLine = True)
#try:
query.show()
if(query.collect()[0].req_type):
    req_type = query.collect()[0].req_type
    if req_type==1:
        #print("************************************************")
        predict_winner()
    elif req_type==2:
        player_profile()
else:
    print("#######################################################")
    match_details()