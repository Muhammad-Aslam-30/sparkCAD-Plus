import os
from datetime import date
import graphviz
import pandas as pd
import json
from collections import defaultdict
from collections import OrderedDict
from copy import copy
import re
import configparser
import queue
from pathlib import Path
import pickle
import ast

config = configparser.ConfigParser()
config.read('config.ini')


class RddAsNode:
    
    def __init__(self, name, is_cached, number_of_usage, number_of_computations):
        self.name = name
        self.is_cached = is_cached
        self.number_of_usage = number_of_usage
        self.number_of_computations = number_of_computations

        
class Rdd:
    
    def __init__(self, id, name, parents_lst, stage_id, job_id, is_cached):
        self.id = id
        self.name = name
        self.parents_lst = parents_lst
        self.stage_id = stage_id
        self.job_id = job_id
        self.is_cached = is_cached
        
    def to_json(self):
        return {
            "id": self.id,
            "name": self.name,
            "parents_lst": self.parents_lst,
            "stage_id": self.stage_id,
            "job_id": self.job_id,
            "is_cached": self.is_cached
        }

    @classmethod
    def from_json(cls, data):
        return cls(
            data["id"],
            data["name"],
            data["parents_lst"],
            data["stage_id"],
            data["job_id"],
            data["is_cached"]
        )
        
class Transformation:
    
    def __init__(self, from_rdd, to_rdd, is_narrow):
        self.from_rdd = from_rdd
        self.to_rdd = to_rdd
        self.is_narrow = is_narrow

    def __eq__(self, other):
        if (isinstance(other, Transformation)):
            return self.from_rdd == other.from_rdd and self.to_rdd == other.to_rdd
        return False

    def __hash__(self):
        return hash(self.from_rdd) ^ hash(self.to_rdd)

    def __lt__(self, other):
        if self.from_rdd == other.from_rdd:
            self.to_rdd < other.to_rdd
        return self.from_rdd < other.from_rdd
    
class TransformationWithoutI:
    
    def __init__(self, from_rdd, to_rdd, is_narrow):
        self.from_rdd = from_rdd
        self.to_rdd = to_rdd
        self.is_narrow = is_narrow
    
class CachingPlanItem:
    
    def __init__(self, stage_id, job_id, rdd_id, is_cache_item):
        self.stage_id = stage_id
        self.job_id = job_id
        self.rdd_id = rdd_id
        self.is_cache_item = is_cache_item

    def __lt__(self, other):
        if self.job_id == other.job_id:
            if self.stage_id == other.stage_id:
                if self.is_cache_item == other.is_cache_item:
                    return self.rdd_id
                return self.is_cache_item
            return self.stage_id < other.stage_id
        return self.job_id < other.job_id


class Utility():
    def get_absolute_path(path):
        if not os.path.isabs(path):
            return str(Path().absolute()) + '/' + path
        return path

    def intersection(lst1, lst2):
        lst3 = [value for value in lst1 if value in lst2]
        return lst3    

    
class FactHub():
    
    app_name = ""
    job_info_dect = {}
    stage_info_dect = {}
    stage_job_dect = {}
    stage_name_dect = {}
    submitted_stage_last_rdd_dect = {}
    stage_no_of_tasks = {}
    stage_i_operator_dect = defaultdict(list)
    stage_i_operators_id = defaultdict(list)
    submitted_stages = set()
    rdds_lst = []
    operator_partition_size = {}
    rddID_in_stage = defaultdict(list)
    stage_operator_partition = {}
    #total = accumulables_update + bytes_written
    stage_total = {}
    job_last_rdd = {}
    job_last_rdd_dect = {}
    job_last_stage = {}
    rdd_id_stage_with_max_tasks = {}
    task_in_which_stage = {}
    rdds_lst_index_dict = {}
    taskid_launchtime = {}
    taskid_finishtime = {}
    taskid_operator_dect = defaultdict(list)
    
    root_rdd_size = {}
    rddID_size = {}
    last_rdd_size = {}
    operator_timestamp = {}
    rdds_lst_renumbered = []
    transformations = []
    
    rdds_lst_refactored = []
    rdds_lst_InstrumentedRdds = []
    rdds_lst_InstrumentedRdds_id = []
    

    def flush():
        FactHub.app_name = ""
        FactHub.job_info_dect = {}
        FactHub.job_last_stage = {}
        FactHub.stage_info_dect = {}
        FactHub.stage_job_dect = {}
        FactHub.stage_name_dect = {}
        FactHub.submitted_stage_last_rdd_dect = {}
        FactHub.job_last_rdd_dect = {}
        FactHub.submitted_stages.clear()
        FactHub.rdds_lst = []
        FactHub.root_rdd_size = {}
        FactHub.rddID_size = {}
        FactHub.last_rdd_size = {}
        FactHub.operator_timestamp = {}
        FactHub.rdds_lst_renumbered = []
        FactHub.rdds_lst_refactored = []
        FactHub.rdds_lst_InstrumentedRdds = []
        FactHub.rdds_lst_InstrumentedRdds_id = []
        
class AnalysisHub():
    
    transformations_set = set()
    rdd_num_of_computations = defaultdict(int)
    rdd_num_of_usage = defaultdict(int)
    anomalies_dict = {}
    stage_computed_rdds = {}
    stage_used_rdds = {}
    computed_rdds = set()
    rdd_usage_lifetime_dict = {}
    caching_plan_lst = []
    memory_footprint_lst = []
    cached_rdds_set = set()
    non_cached_rdds_set = set()
    cached_rdds_lst = []
    rdds_computation_time = []
    recommended_schedule_cache_at = {}
    recommended_schedule_unpersist_after = {}
    transformation_without_i = []
    transformation_from_to = {}

    def flush():
        AnalysisHub.transformations_set.clear()
        AnalysisHub.rdd_num_of_computations = defaultdict(int)
        AnalysisHub.rdd_num_of_usage = defaultdict(int)
        AnalysisHub.anomalies_dict = {}
        AnalysisHub.stage_computed_rdds = {}
        AnalysisHub.stage_used_rdds = {}
        AnalysisHub.computed_rdds.clear()
        AnalysisHub.rdd_usage_lifetime_dict = {}
        AnalysisHub.cached_rdds_lst = []
        AnalysisHub.rdds_computation_time = []
        AnalysisHub.recommended_schedule_cache_at = {}
        AnalysisHub.recommended_schedule_unpersist_after = {}
        AnalysisHub.transformation_without_i = []
        AnalysisHub.transformation_from_to = {}
        
class Parser():    
    
    def prepare(raw_log_file):
        all_events_lst = pd.read_json(raw_log_file, lines=True)
        FactHub.app_name = all_events_lst[all_events_lst['Event'] == 'SparkListenerApplicationStart']['App Name'].tolist()[0]
        Parser.prepare_from_stage_submitted_events(all_events_lst[all_events_lst['Event'] == 'SparkListenerStageSubmitted'])
        Parser.prepare_from_job_start_events(all_events_lst[all_events_lst['Event'] == 'SparkListenerJobStart'])
        Parser.prepare_from_task_end_events(all_events_lst[all_events_lst['Event'] == 'SparkListenerTaskEnd'])
        Parser.prepare_RDD_ID_from_stage_submitted_events(all_events_lst[all_events_lst['Event'] == 'SparkListenerStageSubmitted'])
        Parser.prepare_root_from_stage_completed_events(all_events_lst[all_events_lst['Event'] == 'SparkListenerStageCompleted'])
        Parser.prepare_leaf_from_task_end_events(all_events_lst[all_events_lst['Event'] == 'SparkListenerTaskEnd'])
        Parser.prepare_from_task_end_events_for_timestamp(all_events_lst[all_events_lst['Event'] == 'SparkListenerTaskEnd'])
    
    def prepare_from_task_end_events_for_timestamp(task_end_events):
        rdd = 0
        for task_id in FactHub.taskid_operator_dect.keys():
            #To get stage_id for this task
            stage_id = FactHub.task_in_which_stage[task_id]
            for j in FactHub.job_last_stage.keys():
                if (FactHub.job_last_stage[j] == stage_id):
                    rdd = FactHub.job_last_rdd[j]
            total_entries = 0
            # To calculate total operators count in one task
            for x in FactHub.taskid_operator_dect[task_id]:
                total_entries = total_entries + 1
            for i in range(total_entries):
                if (i == 0):
                    first_operator_time = FactHub.taskid_operator_dect[task_id][i]['Timestamp'] - FactHub.taskid_launchtime[task_id]
                    if FactHub.taskid_operator_dect[task_id][i]['Operator ID'] in FactHub.operator_timestamp.keys():
                        FactHub.operator_timestamp[FactHub.taskid_operator_dect[task_id][i]['Operator ID']] = FactHub.operator_timestamp[FactHub.taskid_operator_dect[task_id][i]['Operator ID']] + first_operator_time
                    else:
                        FactHub.operator_timestamp[FactHub.taskid_operator_dect[task_id][i]['Operator ID']] = first_operator_time
                else:
                    middle_operator_time = FactHub.taskid_operator_dect[task_id][i]['Timestamp'] - FactHub.taskid_operator_dect[task_id][i-1]['Timestamp']
                    if FactHub.taskid_operator_dect[task_id][i]['Operator ID'] in FactHub.operator_timestamp.keys():
                        FactHub.operator_timestamp[FactHub.taskid_operator_dect[task_id][i]['Operator ID']] = FactHub.operator_timestamp[FactHub.taskid_operator_dect[task_id][i]['Operator ID']] + first_operator_time
                    else:
                        FactHub.operator_timestamp[FactHub.taskid_operator_dect[task_id][i]['Operator ID']] = middle_operator_time
                #To calculated timestamp of last rdd of a job (logic = used timestamp of an operator before the last rdd and task's finish time)
                if (i == total_entries-1):
                    last_operator_time = FactHub.taskid_finishtime[task_id] - FactHub.taskid_operator_dect[task_id][i]['Timestamp']
                    if rdd in FactHub.operator_timestamp.keys():
                        FactHub.operator_timestamp[rdd] = FactHub.operator_timestamp[rdd] + first_operator_time
                    else:
                        FactHub.operator_timestamp[rdd] = last_operator_time
        
    def prepare_leaf_from_task_end_events(task_end_events):
        stage_id_for_a_task = task_end_events['Stage ID'].tolist()
        #dictionary for task and the stage it belongs to
        for i, stage in enumerate(stage_id_for_a_task):
            FactHub.task_in_which_stage[i] = int(stage)
        temp_dict = {}
        queue = []
        accumulables_info_list = task_end_events['Task Info'].tolist()
        for i, task_end in enumerate(task_end_events['Task Info'].tolist()):
            FactHub.taskid_launchtime[task_end['Task ID']] = task_end['Launch Time']
            FactHub.taskid_finishtime[task_end['Task ID']] = task_end['Finish Time']
            accumulables_update = 0
            total = 0
            for j, accumulables in enumerate(task_end['Accumulables']):
                if accumulables['Name'] == "internal.metrics.resultSize":
                    accumulables_update = accumulables['Update']
            for k, task_metrics in enumerate(task_end_events['Task Metrics']):
                    if "Bytes Written" in task_metrics['Output Metrics'].keys():
                        bytes_written = task_metrics['Output Metrics']['Bytes Written']
            total = accumulables_update + bytes_written
            queue.append(total)
        for i in FactHub.stage_no_of_tasks:
            total = 0
            for x in range(0, FactHub.stage_no_of_tasks[i]):
                if len(queue)==0:
                    break
                total = total + queue[0]
                queue.pop(0)
            FactHub.stage_total[i] = total
        for job_id in FactHub.job_last_stage:
            last_stage = FactHub.job_last_stage[job_id]
            if last_stage in FactHub.stage_total.keys():
                total = FactHub.stage_total[last_stage]
                FactHub.last_rdd_size[FactHub.job_last_rdd[job_id]] = total
    
    def prepare_root_from_stage_completed_events(stage_submitted_events):
        max_size_root_rdd = 0
        for i, submitted_stage in enumerate(stage_submitted_events['Stage Info'].tolist()):
            stage_id = submitted_stage['Stage ID']
            for j, rdd_info in enumerate(submitted_stage['RDD Info']):
                parent_id = rdd_info['Parent IDs']
                if not parent_id:
                    root_rdd = rdd_info['RDD ID']
            for k, read_bytes in enumerate(submitted_stage['Accumulables']):
                if read_bytes['Name'] == 'internal.metrics.input.bytesRead' and read_bytes['Value'] > max_size_root_rdd:
                    max_size_root_rdd = read_bytes['Value']
                    root_rdd = rdd_info['RDD ID']
        FactHub.root_rdd_size[root_rdd] = max_size_root_rdd
        
    def prepare_RDD_ID_from_stage_submitted_events(stage_submitted_events):
        for i, submitted_stage in enumerate(stage_submitted_events['Stage Info'].tolist()):
            stage_id = submitted_stage['Stage ID']
            for j, rdd_info in enumerate(submitted_stage['RDD Info']):
                rdd_id = rdd_info['RDD ID']
                FactHub.rddID_in_stage[rdd_id].append(stage_id)
        # To order the FactHub.rddID_in_stage in ascending order based on key
        dict1 = OrderedDict(sorted(FactHub.rddID_in_stage.items()))
        for i in dict1:
            max_no_of_tasks = 0
            for j in dict1[i]:
                if FactHub.stage_no_of_tasks[j] > max_no_of_tasks:
                    max_no_of_tasks = FactHub.stage_no_of_tasks[j]
                    respective_stage = j
            FactHub.rdd_id_stage_with_max_tasks[i] = respective_stage
        for operator in FactHub.operator_partition_size:
            if operator in FactHub.rdd_id_stage_with_max_tasks.keys():
                stage_with_max_tasks = FactHub.rdd_id_stage_with_max_tasks[operator]
            for stage in FactHub.stage_i_operators_id:
                if operator in FactHub.stage_i_operators_id[stage]:
                    operator_s_stage = stage
            if operator_s_stage == stage_with_max_tasks:
                FactHub.rddID_size[operator] = FactHub.operator_partition_size[operator]
            else:
                FactHub.rddID_size[operator] = FactHub.operator_partition_size[operator] * (FactHub.stage_no_of_tasks[stage_with_max_tasks])
            
    def prepare_from_job_start_events(job_start_events):
        job_ids_list = job_start_events['Job ID'].tolist()
        job_stage_info_list = job_start_events['Stage Infos'].tolist()
        for job_num, job_rec in enumerate(job_stage_info_list):
            job_id = int(job_ids_list[job_num])
            FactHub.job_info_dect[job_id] = job_rec
            id_of_last_rdd_in_job = -1
            for stage_num, stage_rec in enumerate(job_rec):
                stage_id = int(stage_rec['Stage ID'])
                FactHub.stage_job_dect[stage_id] = job_id
                FactHub.stage_info_dect[stage_id] = stage_rec
                FactHub.stage_name_dect[stage_id] = stage_rec['Stage Name']
                # To print the number of tasks in each stage
                FactHub.stage_no_of_tasks[stage_id] = stage_rec['Number of Tasks']
                id_of_last_rdd_in_stage = -1
                for stage_rdd_num, stage_rdd_rec in enumerate(stage_rec['RDD Info']):
                    rdd_id = stage_rdd_rec['RDD ID']
                    is_cached = stage_rdd_rec['Storage Level']['Use Memory'] or stage_rdd_rec['Storage Level']['Use Disk']
                    FactHub.rdds_lst.append(Rdd(rdd_id, stage_rdd_rec['Name'] + '\n' + stage_rdd_rec['Callsite'], stage_rdd_rec['Parent IDs'], stage_id, job_id, is_cached))
                    if id_of_last_rdd_in_job < rdd_id:
                        id_of_last_rdd_in_job = rdd_id
                    if id_of_last_rdd_in_stage < rdd_id:
                        id_of_last_rdd_in_stage = rdd_id
                if stage_id in FactHub.submitted_stages:
                    FactHub.submitted_stage_last_rdd_dect[stage_id] = id_of_last_rdd_in_stage
            #Dictionary with job and last rdd along with stage name
            FactHub.job_last_rdd_dect[job_id] = (id_of_last_rdd_in_job, stage_rec['Stage Name'])
            #Dictionary with job and last rdd details without stage name
            FactHub.job_last_rdd[job_id] = (id_of_last_rdd_in_job)
        for i in FactHub.job_last_rdd_dect:
            for j in FactHub.stage_job_dect:
                if (FactHub.stage_job_dect[j] == i):
                    FactHub.job_last_stage[i] = j            
    
    def prepare_from_task_end_events(task_end_events):
        task_stage_ids_list = task_end_events['Stage ID'].tolist()
        task_operators_info_list = task_end_events['SparkIOperatorsDetails'].tolist()
        for index, task_operator_rec in enumerate(task_operators_info_list):
            task_stage_id = int(task_stage_ids_list[index])
            FactHub.stage_i_operator_dect[task_stage_id].append(task_operator_rec)  
        a = []
        operator_ids_list = []
        count = 0
        for task_stage_i in FactHub.stage_i_operator_dect.keys():
            length = len(FactHub.stage_i_operator_dect[task_stage_i])
            for lent in range(length):
                # To Print lists of operators details
                a = FactHub.stage_i_operator_dect[task_stage_i][lent]
                FactHub.taskid_operator_dect[count] = a
                count = count + 1
                for a_seperate_list in a:
                    if a_seperate_list['Operator ID'] in FactHub.operator_partition_size.keys():
                        temp_dict_value = FactHub.operator_partition_size[a_seperate_list['Operator ID']]
                        FactHub.operator_partition_size.update({a_seperate_list['Operator ID'] : (a_seperate_list['Partition Size'] + temp_dict_value)})
                    else:
                        FactHub.operator_partition_size[a_seperate_list['Operator ID']] = a_seperate_list['Partition Size']
                        FactHub.stage_i_operators_id[task_stage_i].append(a_seperate_list['Operator ID'])
                FactHub.stage_operator_partition[task_stage_i] = FactHub.operator_partition_size
        
    def prepare_from_stage_submitted_events(stage_submitted_events):
        for index, submitted_stage in enumerate(stage_submitted_events['Stage Info'].tolist()):
            FactHub.submitted_stages.add(submitted_stage['Stage ID'])

    
class Analyzer():

    def is_narrow_transformation(rdd_id, parent_id):
        rdd_stages_set = set()
        parent_stages_set = set()
        for rdd in FactHub.rdds_lst:
            if rdd.id == rdd_id:
                rdd_stages_set.add(rdd.stage_id)
            elif rdd.id == parent_id:
                parent_stages_set.add(rdd.stage_id)
        return len(Utility.intersection(rdd_stages_set, parent_stages_set)) != 0

    def prepare_transformations_lst():
        for rdd in FactHub.rdds_lst:
            for parent_id in rdd.parents_lst:
                AnalysisHub.transformations_set.add(Transformation(rdd.id, parent_id, Analyzer.is_narrow_transformation(rdd.id, parent_id)))

    def add_rdd_and_its_parents_if_it_is_computed_in_stage(rdd_id, stage_id):#recursive
        if rdd_id not in AnalysisHub.stage_used_rdds[stage_id]:
            AnalysisHub.rdd_num_of_usage[rdd_id] += 1
            AnalysisHub.stage_used_rdds[stage_id].add(rdd_id)            
        for rdd in FactHub.rdds_lst:
            if rdd.id == rdd_id: 
                if rdd.is_cached:
                    if rdd_id not in AnalysisHub.rdd_usage_lifetime_dict:
                        AnalysisHub.rdd_usage_lifetime_dict[rdd.id] = (rdd.stage_id, rdd.job_id, rdd.stage_id, rdd.job_id)
                    if AnalysisHub.rdd_usage_lifetime_dict[rdd_id][0] > stage_id:
                        AnalysisHub.rdd_usage_lifetime_dict[rdd.id] = (rdd.stage_id, rdd.job_id, AnalysisHub.rdd_usage_lifetime_dict[rdd_id][2], AnalysisHub.rdd_usage_lifetime_dict[rdd_id][3])
                    if AnalysisHub.rdd_usage_lifetime_dict[rdd_id][2] < stage_id:
                        AnalysisHub.rdd_usage_lifetime_dict[rdd.id] = (AnalysisHub.rdd_usage_lifetime_dict[rdd_id][0], AnalysisHub.rdd_usage_lifetime_dict[rdd_id][1], rdd.stage_id, rdd.job_id)
            if rdd.id == rdd_id: 
                if rdd.stage_id == stage_id:
                    if rdd.is_cached:
                        if rdd_id in AnalysisHub.computed_rdds: #already cached
                            return
                        AnalysisHub.computed_rdds.add(rdd_id) #cached for the first time
                        AnalysisHub.stage_computed_rdds[stage_id].add(rdd_id)
                    else:
                        if rdd_id in AnalysisHub.computed_rdds: #handeling unpersistance
                            AnalysisHub.computed_rdds.remove(rdd_id)
                        AnalysisHub.stage_computed_rdds[stage_id].add(rdd_id)
                    for parent_id in rdd.parents_lst:
                        if Analyzer.is_narrow_transformation(rdd.id, parent_id):
                            Analyzer.add_rdd_and_its_parents_if_it_is_computed_in_stage(parent_id, stage_id)

    def calc_num_of_computations_of_rdds():
        AnalysisHub.rdd_usage_lifetime_dict = {}
        for stage_id in sorted(FactHub.submitted_stage_last_rdd_dect):
            id_of_last_rdd_in_stage = FactHub.submitted_stage_last_rdd_dect[stage_id]
            AnalysisHub.stage_computed_rdds[stage_id] = set()
            AnalysisHub.stage_used_rdds[stage_id] = set()
            Analyzer.add_rdd_and_its_parents_if_it_is_computed_in_stage(id_of_last_rdd_in_stage, stage_id)            
        for stage_id in AnalysisHub.stage_computed_rdds:
            for rdd_id in AnalysisHub.stage_computed_rdds[stage_id]:
                AnalysisHub.rdd_num_of_computations[rdd_id] += 1

    def prepare_anomalies_dict():
        for rdd in FactHub.rdds_lst:
            rdd.name, rdd.is_cached, AnalysisHub.rdd_num_of_usage[rdd.id], AnalysisHub.rdd_num_of_computations[rdd.id]
            if rdd.is_cached and AnalysisHub.rdd_num_of_usage[rdd.id] <= int(config['Caching_Anomalies']['rdds_computation_tolerance_threshold']):
                AnalysisHub.anomalies_dict[rdd.id] = "unneeded cache"
            elif not rdd.is_cached and AnalysisHub.rdd_num_of_computations[rdd.id] > int(config['Caching_Anomalies']['rdds_computation_tolerance_threshold']):
                AnalysisHub.anomalies_dict[rdd.id] = "recomputation"

    def prepare_caching_plan():
        AnalysisHub.caching_plan_lst = []
        for rdd_id, rdd_usage_lifetime in AnalysisHub.rdd_usage_lifetime_dict.items():
            if config['Caching_Anomalies']['include_caching_anomalies_in_caching_plan'] == "true" or rdd_id not in AnalysisHub.anomalies_dict:
                AnalysisHub.caching_plan_lst.append(CachingPlanItem(rdd_usage_lifetime[0], rdd_usage_lifetime[1], rdd_id, True))
                AnalysisHub.caching_plan_lst.append(CachingPlanItem(rdd_usage_lifetime[2], rdd_usage_lifetime[3], rdd_id, False)) 
        AnalysisHub.memory_footprint_lst = []
        incremental_rdds_set = set()
        for caching_plan_item in sorted(AnalysisHub.caching_plan_lst):
            if caching_plan_item.is_cache_item:
                incremental_rdds_set.add(caching_plan_item.rdd_id)
            else:
                incremental_rdds_set.remove(caching_plan_item.rdd_id)
            AnalysisHub.memory_footprint_lst.append((caching_plan_item.job_id, caching_plan_item.stage_id, (incremental_rdds_set.copy())))
            
    def analyze_caching_anomalies():
        print("AnalysisHub.cached_rdds_set")
        print(AnalysisHub.cached_rdds_set)
        for rdd in FactHub.rdds_lst:
            if rdd.id in AnalysisHub.cached_rdds_set:
                rdd.is_cached = True
            if rdd.id in AnalysisHub.non_cached_rdds_set:
                rdd.is_cached = False
        for rdd in FactHub.rdds_lst:
            if rdd.is_cached:
                print("cached rdd id")
                print(rdd.id)
        Analyzer.calc_num_of_computations_of_rdds()
        Analyzer.prepare_anomalies_dict() 
        Analyzer.prepare_caching_plan() 


class SparkDataflowVisualizer():

    def init():
        AnalysisHub.cached_rdds_set.clear()
        AnalysisHub.non_cached_rdds_set.clear()
        FactHub.flush()
        AnalysisHub.flush()
    
    def parse(raw_log_file):
        Parser.prepare(raw_log_file)
        
    def analyze():
        AnalysisHub.flush()
        Analyzer.prepare_transformations_lst()
        Analyzer.analyze_caching_anomalies()

    def rdds_lst_refactor():
        for i, rdd in enumerate(FactHub.rdds_lst):
            if ("InstrumentedRDD" not in rdd.name):
                FactHub.rdds_lst_refactored.append(rdd)
            else:
                FactHub.rdds_lst_InstrumentedRdds_id.append(rdd.id)
                FactHub.rdds_lst_InstrumentedRdds.append(rdd)
        #temporary list 't' will store all the cached rdds ids
        t = []
        for rdd in FactHub.rdds_lst_InstrumentedRdds:
            if rdd.is_cached:
                t.append(rdd.id)
        t = list(dict.fromkeys(t))
        #temporary list 't1' will store the rdds all details with updated cache status
        t1 = []
        for rdd in FactHub.rdds_lst_refactored:
            if rdd.id+1 in t:
                t1.append(Rdd(rdd.id, rdd.name, rdd.parents_lst, rdd.stage_id, rdd.job_id, True))
            else:
                t1.append(rdd)
        #flushing all the details in the 'FactHub.rdds_lst_refactored' list and dump it again with all the details from 't1'
        #in order to update the caching staus in the 'FactHub.rdds_lst_refactored' list and to show it in the DAG
        FactHub.rdds_lst_refactored = []
        for rdd in t1:
            FactHub.rdds_lst_refactored.append(rdd)
            
        temp = []
        for rdd in FactHub.rdds_lst_refactored:
            temp.append(rdd.id)
        temp = list(dict.fromkeys(sorted(temp)))
        temp1 = []
        temp1 = [temp.index(x) for x in temp]
        for x in temp:
            FactHub.rdds_lst_index_dict[x] = temp.index(x)
        for rdd in FactHub.rdds_lst_refactored:
            FactHub.rdds_lst_renumbered.append(Rdd(FactHub.rdds_lst_index_dict[rdd.id],rdd.name, rdd.parents_lst, rdd.stage_id, rdd.job_id, rdd.is_cached))
                    
    def visualize_property_DAG():
        for rdd in FactHub.rdds_lst_refactored:
            if rdd.is_cached:
                AnalysisHub.cached_rdds_lst.append(FactHub.rdds_lst_index_dict[rdd.id])
        AnalysisHub.cached_rdds_lst = list(dict.fromkeys(AnalysisHub.cached_rdds_lst))
        print(AnalysisHub.cached_rdds_lst)
        #AnalysisHub.transformation_from_to has been used in at config['Drawing']['show_rdd_reach_time'], we need that dict in beforehand 
        #thats why the below loops have been implemented before "dot = grahpviz.Digraph" code line
        #to create AnalysisHub.transformation_without_i to contain rdd ids without instrumentation ids but will have duplicates
        for transformation in sorted(AnalysisHub.transformations_set):
            if transformation.to_rdd not in FactHub.rdds_lst_InstrumentedRdds_id and transformation.from_rdd not in FactHub.rdds_lst_InstrumentedRdds_id:
                AnalysisHub.transformation_without_i.append(TransformationWithoutI(transformation.from_rdd, transformation.to_rdd, Analyzer.is_narrow_transformation(transformation.from_rdd, transformation.to_rdd)))
            if transformation.from_rdd in FactHub.rdds_lst_InstrumentedRdds_id:
                AnalysisHub.transformation_without_i.append(TransformationWithoutI(transformation.from_rdd - 1, transformation.to_rdd, Analyzer.is_narrow_transformation(transformation.from_rdd, transformation.to_rdd)))
            if transformation.to_rdd in FactHub.rdds_lst_InstrumentedRdds_id:
                AnalysisHub.transformation_without_i.append(TransformationWithoutI(transformation.from_rdd, transformation.to_rdd - 1, Analyzer.is_narrow_transformation(transformation.from_rdd, transformation.to_rdd)))
        #to refactor AnalysisHub.transformation_without_i to remove duplicates
        temp_lst_for_transformation_without_i = []
        for transformation in AnalysisHub.transformation_without_i:
            if(transformation.to_rdd != transformation.from_rdd):
                temp_lst_for_transformation_without_i.append(transformation)
        AnalysisHub.transformation_without_i.clear()
        for transformation in temp_lst_for_transformation_without_i:
            AnalysisHub.transformation_without_i.append(transformation)
        for transformation in AnalysisHub.transformation_without_i:
            AnalysisHub.transformation_from_to[transformation.from_rdd] = transformation.to_rdd
            
        
        dot = graphviz.Digraph(strict=True, comment='Spark-Application-Graph', format = config['Output']['selected_format'])
        dot.attr('node', shape=config['Drawing']['rdd_shape'], label='this is graph')
        dot.node_attr={'shape': 'plaintext'}
        dot.edge_attr.update(arrowhead='normal', arrowsize='1')
        dag_rdds_set = set()
        prev_action_name = ""
        iterations_count = int(config['Drawing']['max_iterations_count']) 
        for job_id, job in sorted(FactHub.job_last_rdd_dect.items()):
            action_name = job[1]
            draw_iteration_indicator = False
            if action_name == prev_action_name:
                if iterations_count == 0:
                    continue
                iterations_count-=1
            else:
                iterations_count = int(config['Drawing']['max_iterations_count']) 
            for rdd in FactHub.rdds_lst_refactored:
                if rdd.job_id == job_id and rdd.id not in dag_rdds_set:
                    dag_rdds_set.add(rdd.id)
                    node_label = "\n"
                    if config['Drawing']['show_action_id'] == "true":
                        renumbered_rdd_id = FactHub.rdds_lst_index_dict[rdd.id]
                        node_label = "[" + str(renumbered_rdd_id) + "] "
                        #node_label = "[" + str(rdd.id) + "] "
                    if config['Drawing']['show_rdd_name'] == "true":
                        node_label = node_label + rdd.name[:int(config['Drawing']['rdd_name_max_number_of_chars'])]
                    if config['Drawing']['show_rdd_size'] == "true":
                        if rdd.id in FactHub.rddID_size:
                            size_in_mb = FactHub.rddID_size[rdd.id] / 1000000
                            rounded_size = round(size_in_mb,3)
                            node_label = node_label + "\nsize: " + str(rounded_size) + " mb"
                        elif rdd.id in FactHub.root_rdd_size:
                            size_in_mb = FactHub.root_rdd_size[rdd.id] / 1000000
                            rounded_size = round(size_in_mb,3)
                            node_label = node_label + "\nsize: " + str(rounded_size) + " mb"
                        elif rdd.id in FactHub.last_rdd_size:
                            size_in_mb = FactHub.last_rdd_size[rdd.id] / 1000000
                            rounded_size = round(size_in_mb,3)
                            node_label = node_label + "\nsize: " + str(rounded_size) + " mb"
                    if config['Drawing']['show_rdd_reach_time'] == "true":
                        reach_time = 0
                        curr_rdd = rdd.id
                        for x in range(100):
                            if curr_rdd == 0 or curr_rdd not in AnalysisHub.transformation_from_to.keys():
                                break
                            prev_rdd = AnalysisHub.transformation_from_to[curr_rdd]
                            if curr_rdd in FactHub.operator_timestamp.keys():
                                reach_time = reach_time + FactHub.operator_timestamp[curr_rdd]
                            prev_rdd_index = FactHub.rdds_lst_index_dict[prev_rdd]
                            if prev_rdd_index in AnalysisHub.cached_rdds_lst or prev_rdd == 0:
                                break
                            curr_rdd = prev_rdd
                        if reach_time >= 1000:
                            reach_time = reach_time / 1000
                            node_label = node_label + "\nreach time: " + str(reach_time) + " s"
                        else:
                            node_label = node_label + "\nreach time: " + str(reach_time) + " ms"
                    if config['Caching_Anomalies']['show_number_of_rdd_usage'] == "true":
                        node_label = node_label + "\nused: " + str(AnalysisHub.rdd_num_of_usage[rdd.id])
                    if config['Caching_Anomalies']['show_number_of_rdd_computations'] == "true":
                        node_label = node_label + "\ncomputed: " + str(AnalysisHub.rdd_num_of_computations[rdd.id])
                    if  config['Caching_Anomalies']['highlight_unneeded_cached_rdds'] == "true" and AnalysisHub.anomalies_dict.get(rdd.id, "") == "unneeded cache":
                        dot.node(str(rdd.id), penwidth = '3', fillcolor = config['Drawing']['cached_rdd_bg_color'], color = 'red', shape = config['Drawing']['anomaly_shape'], style = 'filled', label = node_label)
                    elif config['Caching_Anomalies']['highlight_recomputed_rdds'] == "true" and AnalysisHub.anomalies_dict.get(rdd.id, "") == "recomputation":
                        dot.node(str(rdd.id), penwidth = '3', fillcolor = 'white', color = 'red', shape = config['Drawing']['anomaly_shape'], style = 'filled', label = node_label)
                    else:
                        dot.node(str(rdd.id), fillcolor = config['Drawing']['cached_rdd_bg_color'] if FactHub.rdds_lst_index_dict[rdd.id] in AnalysisHub.cached_rdds_lst else 'white', style = 'filled', label = node_label)
            action_lable = "" 
            if config['Drawing']['show_action_id'] == "true":
                action_lable = "[" + str(job_id) + "]"
            if config['Drawing']['show_action_name'] == "true":
                action_lable = action_lable + action_name[:int(config['Drawing']['action_name_max_number_of_chars'])]
            if draw_iteration_indicator == True:    
                draw_iteration_indicator = False
                continue
            dot.node("Action_" + str(job_id), shape=config['Drawing']['action_shape'] if iterations_count != 0 else config['Drawing']['iterative_action_shape'], fillcolor = config['Drawing']['action_bg_collor'] if iterations_count != 0 else config['Drawing']['iterative_action_collor'], style = 'filled', label = action_lable)
            dot.edge(str(job[0]), "Action_" + str(job_id), color = 'black', arrowhead = 'none', style = 'dashed')
            prev_action_name = action_name
        
          
        for transformation in AnalysisHub.transformation_without_i:
            if transformation.to_rdd in dag_rdds_set and transformation.from_rdd in dag_rdds_set:
                dot.edge(str(transformation.to_rdd), str(transformation.from_rdd), color = config['Drawing']['narrow_transformation_color'] if transformation.is_narrow else config['Drawing']['wide_transformation_color'])
        for transformation in AnalysisHub.transformation_without_i:
            if transformation.to_rdd in dag_rdds_set and transformation.from_rdd in dag_rdds_set:
                if transformation.from_rdd in FactHub.operator_timestamp:
                    time = FactHub.operator_timestamp[transformation.from_rdd]
                    if time < 1000:
                        dot.edge(str(transformation.to_rdd), str(transformation.from_rdd), label = "  " + str(time) + " ms")
                    else:
                        time = time / 1000
                        rounded_time = round(time,1)
                        dot.edge(str(transformation.to_rdd), str(transformation.from_rdd), label = "  " + str(rounded_time) + " s")
        
        caching_plan_label = "\nRecommended Schedule:\n"
        for caching_plan_item in sorted(AnalysisHub.caching_plan_lst):
            if caching_plan_item.is_cache_item:
                caching_plan_label += "\nCache "
            else:
                caching_plan_label += "\nUnpersist "
            #cache_rdd variable used below will store the correct rdd.id of the instrumented rdd's map partition's id after refactoring & renumbering the rdds list
            cache_rdd = 0
            if caching_plan_item.rdd_id-1 in FactHub.rdds_lst_index_dict.keys():
                cache_rdd = FactHub.rdds_lst_index_dict[caching_plan_item.rdd_id-1]#caching_plan_label += "RDD[" + str(caching_plan_item.rdd_id - 1) + "] " + ("at" if caching_plan_item.is_cache_item else "after") + " stage(" + str(caching_plan_item.stage_id) + ") in job(" + str(caching_plan_item.job_id) + ")\n"
            caching_plan_label += "RDD[" + str(cache_rdd) + "] " + ("at" if caching_plan_item.is_cache_item else "after") + " stage(" + str(caching_plan_item.stage_id) + ") in job(" + str(caching_plan_item.job_id) + ")\n"
        caching_plan_label += "\n"
        if len(AnalysisHub.caching_plan_lst) > 0 and config['Caching_Anomalies']['show_caching_plan'] == "true":
            dot.node("caching_plan", shape = 'note', fillcolor = 'lightgray', style = 'filled', label = caching_plan_label)
        
        memory_footprint_label = "\nMemory Footprint:\n"
        total_size = 0
        temp = set()
        for memory_footprint_item in AnalysisHub.memory_footprint_lst:
            for val in memory_footprint_item[2]:
                temp.add(val-1)
        for val in temp:
                if val in FactHub.rddID_size:
                    size_in_mb = FactHub.rddID_size[val] / 1000000
                    rounded_size = round(size_in_mb,3)
                elif val in FactHub.root_rdd_size:
                    size_in_mb = FactHub.root_rdd_size[val] / 1000000
                    rounded_size = round(size_in_mb,3)
                elif val in FactHub.last_rdd_size:
                    size_in_mb = FactHub.last_rdd_size[val] / 1000000
                    rounded_size = round(size_in_mb,3)
                total_size = total_size + rounded_size
        tempo = set()
        memory_footprint_items = []
        for memory_footprint_item in AnalysisHub.memory_footprint_lst:
            for val in memory_footprint_item[2]:
                tempo.add(FactHub.rdds_lst_index_dict[val-1])
        tempo = list(dict.fromkeys(tempo))
        memory_footprint_label += "\n"
        if len(tempo) == 0:
            memory_footprint_label += "Free"
        else:
            memory_footprint_label += str(tempo)
        memory_footprint_label += "\n"
        memory_footprint_label += "\n"
        memory_footprint_label += "Total size of cached RDDs: " + str(total_size) + " mb"
        memory_footprint_label += "\n"
        memory_footprint_label += "\n"
        if len(AnalysisHub.caching_plan_lst) > 0 and config['Caching_Anomalies']['show_memory_footprint'] == "true":
            dot.node("memory_footprint", shape = 'note', fillcolor = 'lightgray', style = 'filled', label = memory_footprint_label)
        dot.attr(labelloc="t")
        dot.attr(label=FactHub.app_name)
        dot.attr(fontsize='40')
        spark_dataflow_visualizer_output_path = Utility.get_absolute_path(config['Paths']['output_path'])
        output_file_name = re.sub('[^a-zA-Z0-9]+', '', FactHub.app_name)
        dot.render(spark_dataflow_visualizer_output_path + '/' + output_file_name, view=config['Output']['view_after_render'] == 'true')
        
def load_facthub_data(file_name, log_file_path):
    facthubs_folder = "FactHubs"
    file_path = os.path.join(facthubs_folder, file_name+".pickle")
    
    if os.path.exists(file_path):
        #Load the data from the file
        with open(file_path, 'r') as f:
            data = json.load(f)
            FactHub.app_name = data['app_name']
            FactHub.job_info_dect = {int(k): v for k, v in data['job_info_dect'].items()}
            FactHub.stage_info_dect = {int(k): v for k, v in data['stage_info_dect'].items()}
            FactHub.stage_job_dect = {int(k): v for k, v in data['stage_job_dect'].items()}
            FactHub.stage_name_dect = {int(k): v for k, v in data['stage_name_dect'].items()}
            FactHub.submitted_stage_last_rdd_dect = {int(k): v for k, v in data['submitted_stage_last_rdd_dect'].items()}
            FactHub.stage_no_of_tasks = {int(k): v for k, v in data['job_info_dect'].items()}
            FactHub.stage_i_operator_dect = {int(k): v for k, v in data['stage_i_operator_dect'].items()}
            FactHub.stage_i_operators_id = {int(k): v for k, v in data['stage_i_operators_id'].items()}
            FactHub.submitted_stages = set(data['submitted_stages'])
            FactHub.rdds_lst = [Rdd.from_json(rdd) for rdd in data['rdds_lst']]
            FactHub.operator_partition_size = {int(k): v for k, v in data['operator_partition_size'].items()}
            FactHub.rddID_in_stage = {int(k): v for k, v in data['rddID_in_stage'].items()}
            FactHub.stage_operator_partition = {int(k): v for k, v in data['stage_operator_partition'].items()}
            FactHub.stage_total = {int(k): v for k, v in data['stage_total'].items()}
            FactHub.job_last_rdd = {int(k): v for k, v in data['job_last_rdd'].items()}
            FactHub.job_last_rdd_dect = {int(k): v for k, v in data['job_last_rdd_dect'].items()}
            FactHub.job_last_stage = {int(k): v for k, v in data['job_last_stage'].items()}
            FactHub.rdd_id_stage_with_max_tasks = {int(k): v for k, v in data['rdd_id_stage_with_max_tasks'].items()}
            FactHub.task_in_which_stage = {int(k): v for k, v in data['task_in_which_stage'].items()}
            FactHub.rdds_lst_index_dict = {int(k): v for k, v in data['rdds_lst_index_dict'].items()}
            FactHub.taskid_launchtime = {int(k): v for k, v in data['taskid_launchtime'].items()}
            FactHub.taskid_finishtime = {int(k): v for k, v in data['taskid_finishtime'].items()}
            FactHub.taskid_operator_dect ={int(k): v for k, v in data['taskid_operator_dect'].items()}
            FactHub.root_rdd_size = {int(k): v for k, v in data['root_rdd_size'].items()}
            FactHub.rddID_size = {int(k): v for k, v in data['rddID_size'].items()}
            FactHub.last_rdd_size = {int(k): v for k, v in data['last_rdd_size'].items()}
            FactHub.operator_timestamp = {int(k): v for k, v in data['operator_timestamp'].items()}
            FactHub.rdds_lst_renumbered = [Rdd.from_json(rdd) for rdd in data['rdds_lst_renumbered']]
            FactHub.rdds_lst_refactored = [Rdd.from_json(rdd) for rdd in data['rdds_lst_refactored']]
            FactHub.rdds_lst_InstrumentedRdds = [Rdd.from_json(rdd) for rdd in data['rdds_lst_InstrumentedRdds']]
            FactHub.rdds_lst_InstrumentedRdds_id = [int(x) for x in data['rdds_lst_InstrumentedRdds_id']]
    
    else:
        SparkDataflowVisualizer.init()
        SparkDataflowVisualizer.parse(log_file_path)
        SparkDataflowVisualizer.rdds_lst_refactor()
        # Create the FactHubs folder in the current directory if it doesn't exist
        if not os.path.exists(facthubs_folder):
            os.makedirs(facthubs_folder)
        # Serialize the instance data and write it to a file
        data = {
        "app_name": FactHub.app_name,
        "job_info_dect": FactHub.job_info_dect,
        "stage_info_dect": FactHub.stage_info_dect,
        "stage_job_dect": FactHub.stage_job_dect,
        "stage_name_dect": FactHub.stage_name_dect,
        "submitted_stage_last_rdd_dect": FactHub.submitted_stage_last_rdd_dect,
        "stage_no_of_tasks": FactHub.stage_no_of_tasks,
        "stage_i_operator_dect": dict(FactHub.stage_i_operator_dect),
        "stage_i_operators_id": dict(FactHub.stage_i_operators_id),
        "submitted_stages": list(FactHub.submitted_stages),
        "rdds_lst": [rdd.to_json() for rdd in FactHub.rdds_lst],
        "operator_partition_size": FactHub.operator_partition_size,
        "rddID_in_stage": dict(FactHub.rddID_in_stage),
        "stage_operator_partition": FactHub.stage_operator_partition,
        "stage_total": FactHub.stage_total,
        "job_last_rdd": FactHub.job_last_rdd,
        "job_last_rdd_dect": FactHub.job_last_rdd_dect,
        "job_last_stage": FactHub.job_last_stage,
        "rdd_id_stage_with_max_tasks": FactHub.rdd_id_stage_with_max_tasks,
        "task_in_which_stage": FactHub.task_in_which_stage,
        "rdds_lst_index_dict": FactHub.rdds_lst_index_dict,
        "taskid_launchtime": FactHub.taskid_launchtime,
        "taskid_finishtime": FactHub.taskid_finishtime,
        "taskid_operator_dect": dict(FactHub.taskid_operator_dect),
        "root_rdd_size": FactHub.root_rdd_size,
        "rddID_size": FactHub.rddID_size,
        "last_rdd_size": FactHub.last_rdd_size,
        "operator_timestamp": FactHub.operator_timestamp,
        "rdds_lst_renumbered": [rdd.to_json() for rdd in FactHub.rdds_lst_renumbered],
        "rdds_lst_refactored": [rdd.to_json() for rdd in FactHub.rdds_lst_refactored],
        "rdds_lst_InstrumentedRdds": [rdd.to_json() for rdd in FactHub.rdds_lst_InstrumentedRdds],
        "rdds_lst_InstrumentedRdds_id": [str(rdd) for rdd in FactHub.rdds_lst_InstrumentedRdds_id],
        }
        with open(file_path, 'w') as f:
            json.dump(data, f)
        
# Useful functions for the demonstration 

def load_file(file_name):
    spark_dataflow_visualizer_input_path = Utility.get_absolute_path(config['Paths']['input_path'])
    print(spark_dataflow_visualizer_input_path)
    log_file_path = spark_dataflow_visualizer_input_path + '/' + file_name
    load_facthub_data(file_name, log_file_path)

def draw_DAG():
    SparkDataflowVisualizer.analyze() 
    SparkDataflowVisualizer.rdds_lst_refactor()
    SparkDataflowVisualizer.visualize_property_DAG()
    
def cache(rdd_id):
    key = list(FactHub.rdds_lst_index_dict.keys())[list(FactHub.rdds_lst_index_dict.values()).index(rdd_id)]
    AnalysisHub.cached_rdds_set.add(key+1)
    AnalysisHub.non_cached_rdds_set.discard(key+1)
    draw_DAG()
    
def dont_cache(rdd_id):
    AnalysisHub.non_cached_rdds_set.add(rdd_id)
    AnalysisHub.cached_rdds_set.discard(rdd_id)
    draw_DAG()
