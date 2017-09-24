from parsing import *
from connection import *

def read_log():
    with open("recovery.txt", "r") as f:
        logs = []
        undo_list = []
        for idx, log in enumerate(f):
            if log_type_check(log) == "checkpoint":
                checkpoint_idx = idx
                undo_list.extend(log_parse(log))

            if log[-1] == "\n":
                logs.append(log[:-1])

            else:
                logs.append(log)

    return logs, checkpoint_idx, undo_list



def recover_log():
    global undo_list
    logs, checkpoint_idx, undo_list = read_log()

    for log in logs[checkpoint_idx:]:
        redo(log)

    for log in logs[::-1]:
        if undo_list == []:
            break
        undo(log)

    connection.commit()
    connection.close()


def redo(log):
    global undo_list

    if log_type_check(log) == "set":
        transaction, table, key, column, new = log_parse(log)
        pk = primary_keys[table]
        sql = 'update %s set %s = "%s" where %s = "%s"' % (table, column, new, pk, key)
        cursor.execute(sql)

    elif log_type_check(log) == "change":
        transaction, table, key, column, old, new = log_parse(log)
        pk = primary_keys[table]
        sql = 'update %s set %s = "%s" where %s = "%s"' % (table, column, new, pk, key)
        cursor.execute(sql)

    elif log_type_check(log) == "checkpoint":
        pass

    elif log_type_check(log) == "start":
        undo_list.append(log_parse(log))

    elif log_type_check(log) == 'commit':
        undo_list.remove(log_parse(log))

    elif log_type_check(log) == 'abort':
        undo_list.remove(log_parse(log))


def undo(log):
    global undo_list

    if log_type_check(log) == "set":
        pass

    elif log_type_check(log) == "change":
        transaction, table, key, column, old, new = log_parse(log)
        pk = primary_keys[table]
        if transaction in undo_list:
            # log should be followed by sql
            with open("recovery.txt", 'a') as f:
                new_log = "%s %s.%s.%s, %s\n" % (transaction, table, key, column, old)
                f.write(new_log)
            sql = 'update %s set %s = "%s" where %s = "%s"' % (table, column, old, pk, key)
            cursor.execute(sql)


    elif log_type_check(log) == "start":
        transaction = log_parse(log)
        if transaction in undo_list:
            undo_list.remove(log_parse(log))
            with open("recovery.txt", 'a') as f:
                new_log = "%s abort\n" % (transaction)
                f.write(new_log)

    elif log_type_check(log) == 'commit':
        pass

    elif log_type_check(log) == 'abort':
        pass