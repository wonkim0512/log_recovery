import re

primary_keys = {
    'class': 'class_id',
    'course': 'course_id',
    'department': 'dept_id',
    'employee': 'ename',
    'professor': 'prof_id',
    'student': 'stu_id'
}

def log_type_check(log):
    if re.search("checkpoint", log):
        return "checkpoint"

    elif re.search("<T[0-9]+> start", log):
        return "start"

    elif re.search("<T[0-9]+> commit", log):
        return "commit"

    elif re.search("<T[0-9]+> abort", log):
        return "abort"

    elif re.search("<T[0-9]+> commit", log):
        return "commit"

    # set을 먼저하면 change할 것을 set이 먼저 먹어버리는 현상이 발생함.
    elif re.search("<T[0-9]+> .+\..+\..+, .+, .+", log):
        return "change"

    elif re.search("<T[0-9]+> .+\..+\..+, .+", log):
        return "set"



def log_parse(log):
    log_type = log_type_check(log)

    if log_type == "checkpoint":
        transaction = re.findall("<T[0-9]+>", log)
        return transaction

    elif log_type == "set":
        splited = log.split()
        transaction = splited[0]
        table = splited[1].split(".")[0]
        key = splited[1].split(".")[1]
        column = splited[1].split(".")[2].rstrip(",")
        new = splited[2]

        return transaction, table, key, column, new


    elif log_type == "change":
        splited = log.split()
        transaction = splited[0]
        table = splited[1].split(".")[0]
        key = splited[1].split(".")[1]
        column = splited[1].split(".")[2].rstrip(",")
        old = splited[2].rstrip(",")
        new = splited[3]

        return transaction, table, key, column, old, new

    else:
        splited = log.split()
        transaction = splited[0]

        return transaction