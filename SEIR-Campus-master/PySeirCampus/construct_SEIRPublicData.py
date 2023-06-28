import numpy as np
import pandas as pd
from tabulate import tabulate
import random
import pandas as pd
import math
import os

#In this section, we will construct a trip 
#count dataframe from the given file path.
#Then we will sample a trip count given TRAVELER_TYPE, \
#and the trip count dataframe.


def generateJobStatusDataFrames(Tract_acti_file, 
                                BG_acti_files, 
                                total_job_count,
                                job_type_count = 1):
    Tract_acti_df = generateJobStatusDataFrame(Tract_acti_file,
                                               total_job_count,
                                               scale="Tract",
                                               job_type_count=job_type_count)
    BG_acti_dfs = []
    for i in range(len(Tract_acti_df.loc[:,"BLOCK_GROUP_ID"])):
        file = BG_acti_files[i]
        BG_job_count = Tract_acti_df.loc[i,"JOB_COUNT"]
        BG_acti_df = generateJobStatusDataFrame(file, 
                                                BG_job_count, 
                                                scale="BlockGroup",
                                                job_type_count=job_type_count)
        BG_acti_dfs.append(BG_acti_df)
    return Tract_acti_df, BG_acti_dfs


#generate a job status dataframe
#it includes JOB_TYPE, JOB_COUNT, BLOCK_ID
def generateJobStatusDataFrame(ori_job_status_file_path, total_job_count,
                                scale = "BlockGroup", job_type_count = 7):
    #load the files
    file = pd.read_csv(ori_job_status_file_path, header = None)

    series_types = file.iloc[:,0]
    series_ratio = file.iloc[:,1]

    #create block id
    series_id = []
    for i in range(0, len(series_types)):
        series_id.append(math.floor(i/job_type_count))

    #calculate job count for each block
    series_job_count = [round(ratio*total_job_count) for ratio in series_ratio]
    
    scope = None
    if scale == "BlockGroup":
        scope = "BLOCK"
    elif scale == "Tract":
        scope = "BLOCK_GROUP"
    else:
        raise Exception("scale is either 'BlockGroup' or 'Tract'.")

    scope = scope+"_ID"
    #create job status dataframe
    frame = {scope:series_id, "MEETING_TYPE":series_types,\
         "Normalized_Ratio":series_ratio, "JOB_COUNT":series_job_count}
    job_status_df = pd.DataFrame(frame)
    return job_status_df


def getUniJobStatusDataFrame(block_count, total_job_count):
    #create block id
    series_id = [i for i in range(0, block_count)]
    #calculate job count for each block
    series_job_count = [total_job_count/block_count for i in range(0, block_count)]
    series_ratio = [1 / block_count for i in range(0, block_count)]
    series_types = ["Facilities" for i in range(0, block_count)]

    #create job status dataframe
    frame = {"BLOCK_ID":series_id, "MEETING_TYPE":series_types,\
         "Normalized_Ratio":series_ratio, "JOB_COUNT":series_job_count}
    job_status_df = pd.DataFrame(frame)
    return job_status_df


#{'JOB_MEAL': [{'ID': 0, 'JOB_COUNT': 0},
#              {'ID': 1, 'JOB_COUNT': 0},
#              {'ID': 2, 'JOB_COUNT': 0},
#              {'ID': 3, 'JOB_COUNT': 1},...],
#'JOB_SCHOOL': [{'ID': 0, 'JOB_COUNT': 0},
#              {'ID': 1, 'JOB_COUNT': 0},
#              {'ID': 2, 'JOB_COUNT': 0},
#              {'ID': 3, 'JOB_COUNT': 1},...],...}
def get_JOB_TYPE_ID_COUNT(job_status_df, job_type_count = 7, scale="BlockGroup"):
    job_types = job_status_df["MEETING_TYPE"]
    job_type_id_count_dic = {}
    for i in range(0, job_type_count):
        job_id_count_list = get_JOB_ID_COUNT(i, job_status_df, job_type_count, scale)
        job_type_id_count_dic[job_types[i]] = job_id_count_list
    return job_type_id_count_dic


def get_JOB_ID_COUNT(start_index, job_status_df, job_type_count = 7, scale="BlockGroup"):
    JOB_list = []
    scope = None
    if scale == "BlockGroup":
        scope = "BLOCK"
    elif scale == "Tract":
        scope = "BLOCK_GROUP"
    else:
        raise Exception("scale is either 'BlockGroup' or 'Tract'.")

    scope = scope+"_ID"
    for i in range(start_index, len(job_status_df.index),job_type_count):
        dic = {}
        dic[scope] = job_status_df[scope][i]
        dic["Normalized_Ratio"] = job_status_df["Normalized_Ratio"][i]
        JOB_list.append(dic)
    return JOB_list

##############################################################################################

#generate a list of popu dfs from a tract
def generatePopulationDataFrames(Tract_popu_density_file, 
                                BG_popu_density_files, 
                                total_population):
    tract_popu_df = generatePopulationDataFrame(Tract_popu_density_file,total_population, scale="Tract")
    BG_popu_dfs = []
    for i in range(len(tract_popu_df.loc[:,"BLOCK_GROUP_ID"])):
        file = BG_popu_density_files[i]
        BG_popu = tract_popu_df.loc[i,"Population"]
        BG_popu_df = generatePopulationDataFrame(file, BG_popu, scale="BlockGroup")
        BG_popu_dfs.append(BG_popu_df)
    return BG_popu_dfs


def generatePopulationDataFrame(file_path, total_population, scale = "BlockGroup"):
    #load the files
    file = pd.read_csv(file_path, header = None)

    series_ratio = file.iloc[:,1]

    #create block id
    series_id = [i for i in range(0, len(series_ratio))]

    #calculate job count for each block
    series_job_count = [round(ratio*total_population) for ratio in series_ratio]
    
    scope = None
    if scale == "BlockGroup":
        scope = "BLOCK_ID"
    elif scale == "Tract":
        scope = "BLOCK_GROUP_ID"
    else:
        raise Exception("scale is either 'BlockGroup' or 'Tract'.")

    #create job status dataframe
    frame = {scope:series_id, "Normalized_Ratio":series_ratio, "Population":series_job_count}
    job_status_df = pd.DataFrame(frame)
    return job_status_df


##############################################################################################


def construct_roulette_from_normalized_ratio(meeting_and_ratio, 
    total_job_count, 
    total_population):

    Roulette = []
    job_count_sum = 0

    if total_job_count <= 0:
        raise Exception("total job count could not be zero or less than zero.")

    for i in range(0, len(meeting_and_ratio)):
        job_count = meeting_and_ratio[i]["Normalized_Ratio"] * total_job_count
        job_count_sum += job_count
        job_count_sum = round(job_count_sum)
        Roulette.append(job_count_sum)
    
    Roulette.append(total_population)
    return Roulette


#################################################################################

# # this function will create a single route within a Block Group
# def createRouteInsideBG(resident_block_id, BG_population, 
#                 BG_roulette, dest_count_per_trip = 3):
#     #in the scope of a people
#     route = [("HOME", resident_block_id, 1)]
        
#     for i in range(2,dest_count_per_trip):
#         # sample a number from the roulette wheel
#         sampled_number = random.randrange(0, BG_population)
        
#         # Check which roulette part the sampled number is in.
#         # The total number of roulette parts is equal to the number of blocks plus one
#         if sampled_number >= BG_roulette[len(BG_roulette)-2]:
#             # this means this resident will stay at home
#             route.append(("HOME", resident_block_id, i))
#         else:
#             # this means we have to traverse through the roulette wheel
#             # to find out which part the sampled number is in
#             for k in range(0, len(BG_roulette)):
#                 roulette_part_limit = BG_roulette[k]
#                 if sampled_number <= roulette_part_limit:
#                     sampled_meeting_type = "Facilities"
#                     sampled_block_id = k
#                     route.append((sampled_meeting_type, sampled_block_id, i))
#                     break
                    
#     route.append(("HOME", resident_block_id, dest_count_per_trip))
#     return route


# def sampleDestination(route, BG_population, BG_roulette, resident_block_id, resident_block_group_id, i):
#     # sample a number from the roulette wheel
#     sampled_number = random.randrange(0, BG_population)

#     # Check which roulette part the sampled number is in.
#     # The total number of roulette parts is equal to the number of blocks plus one
#     if sampled_number >= BG_roulette[len(BG_roulette)-2]:
#         # this means this resident will stay at home
#         route.append(("HOME", resident_block_id, resident_block_group_id, i))
#     else:
#         # this means we have to traverse through the roulette wheel
#         # to find out which part the sampled number is in
#         for k in range(0, len(BG_roulette)):
#             roulette_part_limit = BG_roulette[k]
#             if sampled_number <= roulette_part_limit:
#                 sampled_meeting_type = "Facilities"
#                 sampled_block_id = k
#                 route.append((sampled_meeting_type, sampled_block_id, resident_block_group_id, i))
#                 break
#     return route


# def createRoute(resident_block_group_id, 
#                 resident_block_id,
#                 BG_total_populations,
#                 outside_prob,
#                 Tract_acti_roulette, 
#                 BG_acti_roulettes_list,
#                 BG_Home_meeting_roulettes_list,
#                 dest_count_per_trip = 3):
#     #in the scope of a people
#     route = [("HOME", resident_block_id, resident_block_group_id, 1)]

#     if_travel_outside_vector = list(np.random.binomial(1, outside_prob, size=dest_count_per_trip-2))
#     #insert 0 to the front and the end of the list
#     if_travel_outside_vector.insert(0, 0)
#     if_travel_outside_vector.append(0)

#     for i in range(1,dest_count_per_trip-1):
#         if_travel_outside = if_travel_outside_vector[i]

#         if if_travel_outside == False:# thie person will travel inside his Block Group
#             BG_population = BG_total_populations[resident_block_group_id]
#             BG_roulette = BG_acti_roulettes_list[resident_block_group_id]
#             route = sampleDestination(route, BG_population, BG_roulette, resident_block_id, resident_block_group_id, i)
#         else:# the person will travel outside his Block Group

#             # sample a number from the Tract_acti_roulette, to determine which BG to go
#             BG_sampled_number = random.randrange(0, Tract_acti_roulette[len(Tract_acti_roulette)-1])
#             # Check which roulette part the BG_sampled_number is in.
#             sampled_Block_Group_id = None
#             for k in range(0, len(Tract_acti_roulette)):
#                 BG_roulette_part_limit = Tract_acti_roulette[k]
#                 if BG_sampled_number <= BG_roulette_part_limit:
#                     sampled_Block_Group_id = k
#                     if resident_block_group_id == sampled_Block_Group_id:
#                         if sampled_Block_Group_id == len(Tract_acti_roulette)-1:
#                             sampled_Block_Group_id -= 1
#                         else:
#                             sampled_Block_Group_id += 1

#             BG_population = BG_total_populations[sampled_Block_Group_id]        
#             BG_roulette = BG_acti_roulettes_list[sampled_Block_Group_id]

#             # sample a number from the Tract_acti_roulette, to determine which BG to go
#             Home_meeting_roulette = BG_Home_meeting_roulettes_list[sampled_Block_Group_id]
#             block_id_sampled_number = random.randrange(0, Home_meeting_roulette[len(Home_meeting_roulette)-1])
#             # Check which roulette part the block_id_sampled_number is in.
#             sampled_Block_id = None
#             for k in range(0, len(Home_meeting_roulette)):
#                 H_roulette_part_limit = Home_meeting_roulette[k]
#                 if block_id_sampled_number <= H_roulette_part_limit:
#                     sampled_Block_id = k

#             route = sampleDestination(route, BG_population, BG_roulette, sampled_Block_id, sampled_Block_Group_id, i)
                    
#     route.append(("HOME", resident_block_id, resident_block_group_id, dest_count_per_trip))
#     return route


#################################################################################
############################################################################

def construct_Single_Meeting(all_trips, 
                             meeting_type, 
                             block_ID, 
                             patronage_seq,
                             start_date="9/2/2020", 
                             end_date="11/13/2020", 
                             duration=30):
                             
    members = []
    for i in range(0, len(all_trips)):
        trip = all_trips[i]
        route = trip["Route"]
        for j in range(0, len(route)):
            destination = route[j][0]
            ID = route[j][1]
            p_seq = route[j][2]
            if destination == meeting_type and ID == block_ID and p_seq==patronage_seq:
                members.append(i)
    
    members = [*set(members)]
    if len(members) <= 5:
        return None
    
    dic0 = {}
    dic0["name"] = meeting_type + " " + str(block_ID) + "."+ str(patronage_seq)
    
    dic1 = {}
    dic1["id"] = dic0["name"]
    dic1["info"] = dic0
    
    days = ["M","T","W","R","F","S","U"]
    dic1["meets"] = [(start_date, end_date, days, duration)]
    
    dic1["members"] = members
    meeting = {}
    meeting["meeting"] = dic1
    
    return meeting


def construct_Meetings_From_Trips(all_trips, 
                                  all_blocks_IDs, 
                                  dest_num,
                                  all_meeting_types, 
                                  meeting_duration,
                                  start_date="9/2/2020", 
                                  end_date="11/13/2020"):

    meetings = []
    for meeting_type in all_meeting_types:
        for block_ID in all_blocks_IDs:
            for seq in range(1, dest_num + 1):
                meeting = construct_Single_Meeting(all_trips, 
                                                   meeting_type, 
                                                   block_ID,
                                                   seq, 
                                                   duration = meeting_duration,
                                                   start_date=start_date, 
                                                   end_date=end_date)

                if meeting != None:
                    meetings.append(meeting)

    return meetings


#construct public.data
def constructSEIR_PublicData(all_trips, 
    all_meeting_types, 
    dest_num,   
    meeting_duration,
    start_date="9/2/2020", 
    end_date="11/13/2020"):

    #get IDs of all blocks
    all_blocks_IDs = []
    temp = set()
    for trip in all_trips:
        start = trip["Route"][0]
        if start not in temp:
            # start is in the format of ('HOME', 0, 1) 
            # this means the meeting is HOME in Block 0 and is the first destination
            # for this traveler
            all_blocks_IDs.append(start[1])
            temp.add(start)

    meetings = construct_Meetings_From_Trips(all_trips,
                                             all_blocks_IDs, 
                                             dest_num,
                                             all_meeting_types, 
                                             meeting_duration,
                                             start_date, 
                                             end_date)
    return meetings


#############################################################################
def createStudent(student_ID, gender="f", is_grad="y", field="undeclared"):
    dic = {}
    student = {}
    student["id"] = student_ID
    student["demographics"] = {}
    student["demographics"]["gender"]=gender
    student["demographics"]["is_grad"]=is_grad
    student["demographics"]["field"]=field
    
    dic["student"]=student
    return dic

def createStudentList(population):
    lst = []
    gender_choice = ["f", "m"]
    is_grad_choice = ["y", "n"]
    field_choice = ["STEM", "business law", "undeclared", "social sciences", "multidisc mixed"]
    for i in range(0, population):
        gender = random.choice(gender_choice)
        is_grad = random.choice(is_grad_choice)
        field = random.choice(field_choice)
        
        student = createStudent(i, gender, is_grad, field)
        lst.append(student)
    return lst