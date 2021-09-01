# coding: utf-8
# create by tongshiwei on 2019-7-5

"""
This script is used to convert the original junyi dataset into json sequence, which can be applied in kt task.
"""

__all__ = ["select_n_most_frequent_students"]

import csv
import json

from longling import wf_open
from longling.lib.candylib import as_list
from tqdm import tqdm


def _read(source: str, ku_dict: str) -> dict:
    """
    Read the learners' interaction records and classify them by user id and session id.
    In the same time, the exercise name will be converted to id.
    """

    outcome = {
        "INCORRECT": 0,
        "CORRECT": 1,
        "HINT": 0,
    }

    students = {}

    with open(ku_dict) as f:
        ku_dict = json.load(f)

    with open(source) as f:
        f.readline()
        for line in tqdm(csv.reader(f, delimiter='\t'), "reading data"):
            student, session, exercise = line[0], line[1], ku_dict[line[-5]],
            correct, timestamp = outcome[line[10]], line[8]
            if student not in students:
                students[student] = {}
            if session not in students[student]:
                students[student][session] = []

            students[student][session].append(
                [int(timestamp), exercise, correct])
    return students


def _write(students, target, max_record_len=200):
    with wf_open(target) as wf:
        for student_id, sessions in tqdm(students.items(), "writing -> %s" % target):
            for session_id, exercises in sessions.items():
                exercises.sort(key=lambda x: x[0])
                exercises_splited = [exercises[i:i + max_record_len]
                                     for i in range(0, len(exercises), max_record_len)]
                for exercises_ in exercises_splited:
                    exercise_response = [(exercise[1], exercise[2])
                                         for exercise in exercises_]
                    print(json.dumps(exercise_response), file=wf)


def _frequency(students, order_by_session=True):
    frequency = {}
    if order_by_session:
        # group by session counts
        for student_id, sessions in tqdm(students.items(), "calculating frequency"):
            frequency[student_id] = len(sessions)
    else:
        # group by record counts
        for student_id, sessions in tqdm(students.items(), "calculating frequency"):
            frequency[student_id] = sum([len(logs)
                                         for session, logs in sessions.items()])
    return sorted(frequency.items(), key=lambda x: x[1], reverse=True)


def get_n_most_frequent_students(students, n=None, frequency: list = None):
    frequency = _frequency(students) if frequency is None else frequency
    __frequency = frequency if n is None else frequency[:n]
    _students = {}
    for _id, _ in __frequency:
        _students[_id] = students[_id]
    return _students


def select_n_most_frequent_students(source: str, target_prefix: str, ku_dict_path: str, n: (int, list), order_by_session: bool, max_record_len: int):
    """
    Read the learners' interaction records and select the records of students who answered questions most frequently,
    then sort the record by timestamp to create sequence(in json format).
    Finally write the sequence into a new file named student_log_kt_n by default.

    Parameters
    ----------
    source: str
        source file path
    tar_root: str
        targat file path
    ku_dict_path: str
        knowledge unit file path
    n:
        None: generate the sequence records of all students
        int : generate the sequence records of the top n students
    order_by_session:
        whether to order the student record by session counts
        A learning process represented by a session_id will produce at least one answer record.
        True:
            order by session counts
        False:
            order by records counts
    max_record_len:int=200
            Maximum length of answer records in a single sample
    -------
    """
    n_list = as_list(n)
    students = _read(source, ku_dict_path)
    frequency = _frequency(students, order_by_session=False)
    for _n in n_list:
        _write(get_n_most_frequent_students(students, _n, frequency),
               target_prefix + "%s" % _n, max_record_len)
