#BD_0133_0254_0406_2029

import json
import sys
from datetime import datetime

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit
from pyspark.streaming import StreamingContext

'''
{"status": "Played", "roundId": 4405654, "gameweek": 2, "teamsData": 
    {"1610": 
        {"scoreET": 0, "coachId": 272869, "side": "away", "teamId": 1610, "score": 2, "scoreP": 0, "hasFormation": 1, 
        "formation": 
            {"bench": 
                [{"playerId": 3551, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 212651, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 291591, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 291594, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 254898, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 3360, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 38093, "ownGoals": "1", "redCards": "0", "goals": "0", "yellowCards": "0"}], 
            "lineup": 
                [{"playerId": 105333, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 3324, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 8625, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 8032, "ownGoals": "0", "redCards": "0", "goals": "2", "yellowCards": "87"}, 
                {"playerId": 135103, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 25553, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 14748, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "30"}, 
                {"playerId": 7892, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "38"}, 
                {"playerId": 31528, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 28291, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 3429, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}], 
            "substitutions": 
                [{"playerIn": 3360, "playerOut": 105333, "minute": 78}, 
                {"playerIn": 38093, "playerOut": 3324, "minute": 79}]}, 
        "scoreHT": 1}, 
    "1624": 
        {"scoreET": 0, "coachId": 292863, "side": "home", "teamId": 1624, "score": 1, "scoreP": 0, "hasFormation": 1, 
        "formation": 
            {"bench": 
                [{"playerId": 240070, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 402884, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 77536, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 65254, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 25804, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 14911, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 269676, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}], 
            "lineup": 
                [{"playerId": 210044, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "31"}, 
                {"playerId": 136441, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 8945, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 36, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "83"}, 
                {"playerId": 8717, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "88"}, 
                {"playerId": 54, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 13484, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 48, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "51"}, 
                {"playerId": 11152, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 61967, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}, 
                {"playerId": 25381, "ownGoals": "0", "redCards": "0", "goals": "0", "yellowCards": "0"}], 
            "substitutions": 
                [{"playerIn": 14911, "playerOut": 210044, "minute": 68}, 
                {"playerIn": 25804, "playerOut": 136441, "minute": 80}, 
                {"playerIn": 269676, "playerOut": 8945, "minute": 90}]}, 
        "scoreHT": 0}}, 
    "seasonId": 181150, "dateutc": "2017-08-20 15:00:00", "winner": 1610, "venue": "Wembley Stadium", "wyId": 2499737, 
    "label": "Tottenham Hotspur - Chelsea, 1 - 2", "date": "August 20, 2017 at 5:00:00 PM GMT+2", 
    "referees": 
        [{"refereeId": 378952, "role": "referee"}, 
        {"refereeId": 385005, "role": "firstAssistant"},
         {"refereeId": 386718, "role": "secondAssistant"},
          {"refereeId": 385909, "role": "fourthOfficial"}], 
          "duration": "Regular", "competitionId": 364}
'''
##########################################################################################################################################
#Returns True if rdd is Match
def filter_by_Match(rdd):
    record_file  = json.loads(rdd)
    a=True
    b=False
    try:
        temp =record_file["wyId"]
        #print(record)
        return a
    except:
        return b
##########################################################################################################################################
#Returns True if rdd is Event
def filter_by_Event(rdd):
    record_file = json.loads(rdd)
    a=True
    b=False
    try:
        temp = record_file["eventId"]
        return a
    except:
        return b
##########################################################################################################################################
#Calculates event characteristics for each player
def calculate_Events(rdd):
    '''
    {"eventId": 8, "subEventName": "Simple pass", 
        "tags": 
            [{"id": 1801}], "playerId": 8325, 
                "positions": [{"y": 53, "x": 49}, {"y": 51, "x": 36}], 
                "matchId": 2499720, "eventName": "Pass", "teamId": 1625, "matchPeriod": "1H", 
                "eventSec": 3.3586760000000027, "subEventId": 85, "id": 178147292}
    '''
    record = json.loads(rdd)
    player_Id = record["playerId"]
    match_Id = record["matchId"]
    event_Id = record["eventId"]
    team_Id = record["teamId"]
    subevent=record["subEventId"]
    #Tags = record["tags"]
    tags = [i["id"] for i in record["tags"]]
    #print(Tags)
    acc_pass=0
    in_acc_pass=0
    key_acc_pass=0
    key_inacc_pass=0
    dual_lost=0
    dual_nuetral=0
    dual_won=0
    fk_acc=0
    fk_unacc=0
    penalty = 0
    on_target=0
    not_on_target=0
    goal=0
    fouls = 0
    own_goal = 0

    if(event_Id == 8):
        flag = 0
        if (1801 in tags):
            flag=1
            acc_pass=acc_pass+1
        if (1802 in tags):
            flag=2
            in_acc_pass=in_acc_pass+1
        if (302 in tags):
            if(flag==1):
                key_acc_pass=key_acc_pass+1
            elif(flag==2):
                key_inacc_pass=key_inacc_pass+1
           
    elif(event_Id == 1):
        if(701 in tags):
            dual_lost +=1
        if(702 in tags):
            dual_nuetral +=1
        if(703 in tags):
            dual_won +=1


    elif (event_Id == 3):
        if(101 in tags):
            if subevent==35:
                penalty += 1
        if(1801 in tags):
            fk_acc=fk_acc+1
        if(1802 in tags):
            fk_unacc=fk_unacc+1

    elif(event_Id == 10):
        if(101 in tags):
            goal +=1
        if(1801 in tags):
            on_target +=1
        if(1802 in tags):
            not_on_target +=1


    elif(event_Id == 2):
        fouls+=1

    if(102 in tags):
        own_goal +=1

    return (player_Id, ((player_Id, match_Id, team_Id), (acc_pass, in_acc_pass, key_acc_pass, key_inacc_pass), (dual_lost ,dual_won, dual_nuetral) ,
             (fk_acc, fk_unacc, penalty), (on_target, not_on_target, goal), (fouls) , (own_goal) ))
##########################################################################################################################################
#Cumulates the event characters with Key: playerId
def cummulate_Metrics(new, old):
    # (8285, ((8285, 2499728, 1627), (1, 0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 0), 0, 0))
    acc_pass=0
    in_acc_pass=0
    key_acc_pass=0
    key_inacc_pass=0

    dual_lost=0
    dual_nuetral=0
    dual_won=0

    fk_acc=0
    fk_unacc=0
    penalty = 0

    on_target=0
    not_on_target=0
    goal=0

    fouls = 0

    own_goal = 0
     
    player_Id = 0
    match_Id = 0
    team_Id = 0
    
    for metric in new:
        player_Id = metric[0][0]
        match_Id = metric[0][1]
        team_Id = metric[0][2]
        acc_pass += metric[1][0]
        in_acc_pass += metric[1][1]
        key_acc_pass += metric[1][2]
        key_inacc_pass += metric[1][3]
        dual_lost += metric[2][0]
        dual_nuetral += metric[2][1]
        dual_won += metric[2][2]
        fk_acc += metric[3][0]
        fk_unacc += metric[3][1]
        penalty += metric[3][2]
        on_target += metric[4][0]
        not_on_target += metric[4][1]
        goal += metric[4][2]
        fouls += metric[5]
        own_goal += metric[6]
    
    if (old is None) or (old[0][1] != match_Id):
    
        return  ((player_Id, match_Id, team_Id), (acc_pass, in_acc_pass, key_acc_pass, key_inacc_pass), (dual_lost ,dual_won, dual_nuetral) ,
            (fk_acc, fk_unacc, penalty), (on_target, not_on_target, goal), (fouls) , (own_goal) )
    else:
        #player_Id = old[0][0]
        #match_Id = old[0][1]
        #team_Id = old[0][2]
        acc_pass += old[1][0]
        in_acc_pass += old[1][1]
        key_acc_pass += old[1][2]
        key_inacc_pass += old[1][3]
        dual_lost += old[2][0]
        dual_nuetral += old[2][1]
        dual_won += old[2][2]
        fk_acc += old[3][0]
        fk_unacc += old[3][1]
        penalty += old[3][2]
        on_target += old[4][0]
        not_on_target += old[4][1]
        goal += old[4][2]
        fouls += old[5]
        own_goal += old[6]
    
        return ( ((player_Id , match_Id, team_Id), (acc_pass, in_acc_pass, key_acc_pass, key_inacc_pass), (dual_lost ,dual_won, dual_nuetral) ,
            (fk_acc, fk_unacc, penalty), (on_target, not_on_target, goal), (fouls) , (own_goal) ))
##########################################################################################################################################
#Calculates the 6 metrics for each player
def calculate_Metrics(new,old):
    metric = new[0]
   
    player_Id = metric[0][0]
    match_Id = metric[0][1]
    team_Id = metric[0][2]
    acc_pass = metric[1][0]
    in_acc_pass = metric[1][1]
    key_acc_pass = metric[1][2]
    key_inacc_pass = metric[1][3]
    dual_lost = metric[2][0]
    dual_nuetral = metric[2][1]
    dual_won = metric[2][2]
    fk_acc = metric[3][0]
    fk_unacc = metric[3][1]
    penalty = metric[3][2]
    on_target = metric[4][0]
    not_on_target = metric[4][1]
    goal = metric[4][2]
    fouls = metric[5]
    own_goal = metric[6]

    try:
        pass_accuracy=float((acc_pass-key_acc_pass+(key_acc_pass*2)))/float((acc_pass+in_acc_pass-key_acc_pass-key_inacc_pass+(key_acc_pass+key_inacc_pass)*2))
    except:
        pass_accuracy = 0.0
    try:
        deul_effeciency=float((dual_won + (dual_nuetral*0.5)))/float((dual_won+dual_lost+dual_nuetral))
    except:
        deul_effeciency = 0.0
    try:
        fk_effectiveness=float(fk_acc+penalty)/float(fk_acc+fk_unacc)
    except:
        fk_effectiveness=0.0
    try:
        shots_target=float((goal+((on_target-goal)*0.5)))/float(on_target+not_on_target+goal)
    except: 
        shots_target = 0.0
    
    return ((player_Id , match_Id, team_Id) , (pass_accuracy, deul_effeciency, fk_effectiveness, shots_target, fouls, own_goal,goal))
##########################################################################################################################################
#Calulates the time each player was on the field
def player_list(rdd):
    player_json_data=json.loads(rdd)
    group_teams_data=player_json_data['teamsData']
    for i in group_teams_data:
        data_team=group_teams_data[i]
        substitution_data= group_teams_data[i]['formation']['substitutions']

        Players_inc=[]
        Players_out=[]
        st=[]
        final_data=[]
        for j in substitution_data:
            Players_inc.append(j["playerIn"])
            Players_out.append(j["playerOut"])
            st.append(j["minute"])
        playing_11=[]
        not_playing_11=[]
        for j in data_team['formation']['lineup']:
            playing_11.append(j['playerId'])
        for j in data_team['formation']['bench']:
            not_playing_11.append(j['playerId'])
        for j in playing_11:
            try:
                final_data.append((j,(0,st[Players_out.index(j)],(st[Players_out.index(j)]),j)))
            except:
                final_data.append((j,(0,90,90,j)))
        for j in not_playing_11:
            try:
                final_data.append((j,(st[Players_inc.index(j)],90,90-st[Players_inc.index(j)],j)))
            except:
                final_data.append((j,(-1,-1,-1,j)))
    return final_data
##########################################################################################################################################
#Updates the rating of each player based on match performance
def RateplayerUpdate(new_value,old_value):
    """
    (8032,(((8032, 2499721, 1610), (0.8974358974358975, 0.56, 1.0, 0.16666666666666666, 2, 0,goal)), (0, 90, 90)))
    ((player_Id , match_Id, team_Id) , (pass_accuracy, deul_effeciency, fk_effectiveness, shots_target, fouls, own_goal)  )
    new[1]=(((8032, 2499721, 1610), (0.8974358974358975, 0.56, 1.0, 0.16666666666666666, 2, 0)), (0, 90, 90))
    new[1][0]=((8032, 2499721, 1610), (0.8974358974358975, 0.56, 1.0, 0.16666666666666666, 2, 0))
    new[1][0][0]=(8032, 2499721, 1610)

    ((8032, 2499721, 1610), (0.8974358974358975, 0.56, 1.0, 0.16666666666666666, 2, 0)), (0, 90, 90)
    """

    try:
        team_id=new_value[0][0][0][2]
        match_id=new_value[0][0][0][1]
        player_id=new_value[0][0][0][0]

        time_spent=new_value[0][1][2]

        pass_accuracy=new_value[0][0][1][0]
        deul_effeciency=new_value[0][0][1][1]
        fk_effectiveness=new_value[0][0][1][2]
        shots_target=new_value[0][0][1][3]
        fouls=new_value[0][0][1][4]
        own_goal=new_value[0][0][1][5]

        goal=new_value[0][0][1][-1]

        #return (player_id)
        if (old_value is None):
            Old_Rating=0.5
        else:
            Old_Rating = old_value[1]
        temp_value=pass_accuracy+deul_effeciency+shots_target+fk_effectiveness
        
        #print(temp_value)
        #print("###########################################")
        player_contribution=float(temp_value)/float(4)
        #print(player_contribution)
        #print("###########################################")
        
        if(time_spent == 90):
            player_contribution = float(player_contribution) * 1.05
        else:
            player_contribution = float(player_contribution) * float(time_spent)/float(90)

        #print(time_spent)
        #print("###########################################")
        Performance = player_contribution - ((0.005 * fouls) * player_contribution)
        Performance = player_contribution - ((0.05 * own_goal) * player_contribution)
        #print("###########################################")

        #print(Performance)
        #print("###########################################")
        New_Rating = float(Performance + Old_Rating) / float(2)

        Change_in_Rating = New_Rating - Old_Rating
        return (player_id, New_Rating , Change_in_Rating, team_id)

    except:
        return old_value
##########################################################################################################################################
#Updates the rating of each player based on match performance
def profileplayerUpdate(new_value,old_value):
    try:
        #print(new_value)
        #print(old_value) 
        if(old_value is None):
            player_Id = new_value[0][0][0]
            fouls = new_value[0][1][4]
            goals = new_value[0][1][6]
            own_goals = new_value[0][1][5]
            pass_accuracy = new_value[0][1][0]
            shots_target = new_value[0][1][3]
        else:
            player_Id = new_value[0][0][0]
            new_fouls=new_value[0][1][4] + old_value[0][1]
            new_goals=new_value[0][1][6] + old_value[0][2]
            new_own_goals=new_value[0][1][5] + old_value[0][3]
            new_pass_accuracy=new_value[0][1][0] + old_value[0][3] 
            new_shots_target= new_value[0][1][3] + old_value[0][5]
            fouls = new_fouls
            goals = new_goals
            own_goals = new_own_goals
            pass_accuracy =new_pass_accuracy
            shots_target = new_shots_target
        
        return (player_Id,fouls,goals,own_goals, pass_accuracy ,shots_target)
    except:
        return old_value    
##########################################################################################################################################
#Calculates the chemistry between a set of players
def chem_calculate(rdd):
    dictionary_val=rdd.collect()
    #print(dictionary_val)
    #print("################################################")
    for i in dictionary_val:
        for j in dictionary_val:
            if(i!=j and i[1][0]!=0 and j[1][0]!=0):
                new_rating=i[1][1]
                Change_in_chem=0

                if (i[1][3]==j[1][3]):
                    Change_in_chem=float(abs((i[1][2]) + (j[1][2])))/float(2)
                    if ((i[1][2]<0 and j[1][2]<0) or (i[1][2]>0 and j[1][2]>0)):
                        Change_in_chem=Change_in_chem
                    else:
                        Change_in_chem=-(Change_in_chem)

                else:
                    Change_in_chem=float(abs((i[1][2]) + (j[1][2])))/float(2)
                    if ((i[1][2]<0 and j[1][2]<0) or (i[1][2]>0 and j[1][2]>0)):
                        Change_in_chem=-Change_in_chem
                    else:
                         Change_in_chem=Change_in_chem

                try:
                    player_combi[str(str(i[0])+" "+str(j[0]))]=str(float(player_combi[str(str(i[0])+" "+str(j[0]))])+Change_in_chem)
                    #print("UPDATED VALUES",player_combi[str(str(i[0])+" "+str(j[0]))])
                except:
                    player_combi[str(str(j[0])+" "+str(i[0]))]=str(float(player_combi[str(str(j[0])+" "+str(i[0]))])+Change_in_chem)
                    #print("UPDATED VALUES",player_combi[str(str(j[0])+" "+str(i[0]))])
##########################################################################################################################################
#Extract details of match from every match rdd
def extract_details(rdd):
    input_data = json.loads(rdd)
    match_Id = input_data["wyId"]
    date = input_data["dateutc"].split()[0]
    label = input_data["label"]
    out_match={}
    out_match['date']=input_data['dateutc'].split()[0]
    out_match['label']=input_data['label']
    out_match['duration']=input_data['duration']
    out_match['winner']=input_data['winner'] # winner is teamID. what happens if draw??
    out_match['venue']=input_data['venue']
    out_match['gameweek']=input_data['gameweek']
    out_match['yellow_cards']=[]
    out_match['red_cards']=[]
    out_match['own_goals']=[]
    out_match['goals']=[]


    teams_data=input_data['teamsData']

    team_1_id=list(teams_data.keys())[0]
    team_2_id=list(teams_data.keys())[1]

    #team 1 info
    team_1=teams_data[list(teams_data.keys())[0]]
    team_1_formation=team_1['formation']
    team_1_bench=team_1_formation['bench']
    team_1_lineup=team_1_formation['lineup']

    #team 2 info
    team_2=teams_data[list(teams_data.keys())[1]]
    team_2_formation=team_2['formation']
    team_2_bench=team_2_formation['bench']
    team_2_lineup=team_2_formation['lineup']

    # processing red and yellow cards for team1
    for row in team_1_bench:
        if row['yellowCards']!='0':
            out_match['yellow_cards'].append(row['playerId'])
        if row['redCards'] != '0':
            out_match['red_cards'].append(row['playerId'])
        if row['goals']=='null':
            row['goals']='0'
        if row['goals']!='0':
            out_match['goals'].append([row['playerId'],team_1_id,int(row['goals'])])
        if row['ownGoals']=='null':
            row['ownGoals']='0'
        if row['ownGoals']!='0':
            out_match['own_goals'].append([row['playerId'],team_1_id,int(row['ownGoals'])])

    for row in team_1_lineup:
        if row['yellowCards']!='0':
            out_match['yellow_cards'].append(row['playerId'])
        if row['redCards']!='0':
            out_match['red_cards'].append(row['playerId'])
        if row['goals']=='null':
            row['goals']='0'
        if row['ownGoals']=='null':
            row['ownGoals']='0'
        if row['goals']!='0':
            out_match['goals'].append([row['playerId'],team_1_id,int(row['goals'])])
        if row['ownGoals']!='0':
            out_match['own_goals'].append([row['playerId'],team_1_id,int(row['ownGoals'])])



    #processing goals for team2
    for row in team_2_bench:
        if row['goals']=='null':
            row['goals']='0'
        if row['ownGoals']=='null':
            row['ownGoals']='0'
        if row['goals']!='0' or row['goals']!='null':
            out_match['goals'].append([row['playerId'],team_2_id,int(row['goals'])])
        if row['ownGoals']!='0' or row['ownGoals']!='null':
            out_match['own_goals'].append([row['playerId'],team_2_id,int(row['ownGoals'])])
        if row['yellowCards']!='0':
            out_match['yellow_cards'].append(row['playerId'])
        if row['redCards']!='0':
            out_match['red_cards'].append(row['playerId'])

    for row in team_2_lineup:
        if row['goals']=='null':
            row['goals']='0'
        if row['ownGoals']=='null':
            row['ownGoals']='0'
        if row['goals']!='0' or row['goals']!='null':
            out_match['goals'].append([row['playerId'],team_2_id,int(row['goals'])])
        if row['ownGoals']!='0' or row['ownGoals']!='null':
            out_match['own_goals'].append([row['playerId'],team_2_id,int(row['ownGoals'])])
        if row['yellowCards']!='0':
            out_match['yellow_cards'].append(row['playerId'])
        if row['redCards']!='0':
            out_match['red_cards'].append(row['playerId'])
    
    
    return ((date,label), (out_match))
##########################################################################################################################################
#Converting match rdd into pair rdd
def join_details(new,old):
    if old is None:
        return (new)
    else:
        return (old)
##########################################################################################################################################
#COunting the number of matches played by each player
def countmatch(new,old):
    try:
        player_id=new[0][-1]
        if(old is None):
            if(new[0][0]!=-1 and new[0][1]!=-1 and new[0][2]!=-1):
                return (player_id,0)
        else:
            count=old[1]+1
            return(player_id,count)
    except:
        return old



##########################################################################################################################################
#DEFINING SPARK SESSION
##########################################################################################################################################
conf = SparkConf()
conf.setAppName("FPL")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
ssc = StreamingContext(sc, 5)
ssc.checkpoint("Checkpointing_done")
lines = ssc.socketTextStream("localhost", 6100)
#lines.pprint()

##########################################################################################################################################
#Initialising Player Chemistry List
players_df = sqlContext.read.load("/user/arshgoyal/csv/players.csv", format="csv", header="true", inferSchema="true")
unique_players=players_df.select('Id').distinct().collect()
unique_list=[]
player_combi=dict()
for i in unique_players:
    unique_list.append(i.Id)
for i in range(len(unique_list)):
    for j in range(i,len(unique_list)):
        if(i!=j):
            player_combi[str(str(unique_list[i])+" "+str(unique_list[j]))]=str(0.5)
##########################################################################################################################################

##########################################################################################################################################
print("########################################################Filter By Match##########################################")
match_data = lines.filter(filter_by_Match)
match_data.pprint()

print("########################################################Filter By Event##########################################")
event_data = lines.filter(filter_by_Event)
event_data.pprint()

print("########################################################Calculate Events#########################################")
event_characteristics = event_data.map(calculate_Events)
event_characteristics.pprint()

print("########################################################Cummulative Metrics##########################################")
metrics = event_characteristics.updateStateByKey(cummulate_Metrics)
metrics.pprint(30)

print("########################################################FINAL METRICS##########################################")
final_metrics = metrics.updateStateByKey(calculate_Metrics)
final_metrics.pprint()

print("########################################################PLAYER DETAILS##########################################")
player_details=match_data.flatMap(lambda y: player_list(y))
player_details.pprint()

print("########################################################MATCHES PLAYERS##########################################")
count_number_mat_played=player_details.updateStateByKey(countmatch)
count_number_mat_played.pprint()

print("########################################################PLAYER DETAILS##########################################")
player_D=final_metrics.join(player_details)
player_D.pprint()

print("########################################################PLAYER RATING##########################################")
player_rate=player_D.updateStateByKey(RateplayerUpdate)
player_rate.pprint()

print("########################################################PLAYER PROFILE ##########################################")
playerprofile=final_metrics.updateStateByKey(profileplayerUpdate)
playerprofile.pprint()

print("########################################################FLUSH DATA OF VARIABLES##########################################")
metrics = metrics.updateStateByKey(lambda x: None)
final_metrics = final_metrics.updateStateByKey(lambda x: None)

print("########################################################CHEMISTRY##########################################")
player_chem=player_rate.foreachRDD(chem_calculate)
#player_chem.pprint()

print("########################################################MATCH DETAILS##########################################")
match_details = match_data.map(extract_details)
match_details.pprint()

print("########################################################ALL MATCHES##########################################")
all_matches = match_details.updateStateByKey(join_details)
#all_matches.pprint()
##########################################################################################################################################



##########################################################################################################################################
#Saving Player_Profile to hdfs
def save_Profile(record):
	df=record.toDF(['Id','metrics'])
	df.show()
	df.coalesce(1).write.json("Player_Profile.json","overwrite")

#Saving Player_Rating to hdfs
def save_Rating(record):
	df=record.toDF(['Id','ratings'])
	df.show()
	df.coalesce(1).write.json("Player_Rating.json","overwrite")

#Saving Match_Details to hdfs
def save_Matches(record):
	df=record.toDF(['match_Id','details'])
	df.show()
	df.coalesce(1).write.json("Match_details.json","overwrite")

#Saving Count_Matches to hdfs
def save_Number(rdd):
    df=rdd.toDF(['id','metric'])
    df.show()
    df.coalesce(1).write.json("Count_Matches.json","overwrite")
##########################################################################################################################################
playerprofile.foreachRDD( lambda rdd: save_Profile(rdd))
player_rate.foreachRDD (lambda rdd: save_Rating(rdd))
all_matches.foreachRDD( lambda rdd: save_Matches(rdd))
count_number_mat_played.foreachRDD(lambda rdd: save_Number(rdd))
##########################################################################################################################################

##########################################################################################################################################
#SPARK STREAMING
##########################################################################################################################################

ssc.start()
ssc.awaitTermination()  
ssc.stop()
with open("/home/arshgoyal/Desktop/BD_Proj/chemistry.json", "w") as outfile:  
     json.dump(player_combi, outfile)