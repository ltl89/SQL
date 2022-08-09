from json.tool import main
from lstore.db import Database
from lstore.query import Query

def main():
    db = Database()

    print("Remark: select data that does not exist is not be implemented.")
    while True:
        user_input = input('>>>')
        if user_input == '' or user_input == ';':
            continue
        if user_input[len(user_input) - 1] != ";":
            print("Request is not a complete Query")
            continue
        user_input = user_input[:-1]
        inputs = user_input.split()
        if inputs[0] not in ['create','drop','insert','select','delete','update','sum']:
            print("Not a valid request")
            continue
        if inputs[0] == "create":
            if inputs[1] == "table":
            ##check if is able to be created
                if len(inputs) == 3:
                    demo_table = db.create_table(inputs[2], 5, 0)
                    if demo_table == False:
                        print('Error: this table name has been used.')
                    else:
                        print("Remark: 5 column tables for demo has been created.")
                else: 
                    print("Usage: create table name_of_table;")
                    continue
            elif inputs[1] == "index":
            ##check if is able to be created
                if len(inputs) == 4:
                    demo_table = db.get_table(inputs[2])
                    if demo_table == False:
                        print('Error: table not exist.')
                    else:
                        demo_table.index.create_index(int(inputs[3]))
                else: 
                    print("Usage: create table name_of_table;")
                    continue
            else:
                print("Usage: create table name_of_table; \nor: create index name_of_table num_column;")
                continue
        ##drop table
        elif inputs[0] == "drop":
            if inputs[1] == "table":
                if len(inputs) == 3:
                    code = db.drop_table(inputs[2])
                    if not code:
                        print('no such table.')
                    continue
            print("Usage: drop table name_of_table;")
        ##insert data
        elif inputs[0] == "insert":
            if inputs[1] == "into":
                if inputs[3][0] == '(' and inputs[3][-1] == ')':
                    if len(inputs) == 4:
                        target_table = db.get_table(inputs[2])
                        if target_table == False:
                            print('Error: no such table exist.')
                            continue
                        query = Query(target_table)
                        temp = inputs[3][1:-1]
                        value_str_list = temp.split(',')
                        value_int_list = [int(i) for i in value_str_list]
                        if len(value_int_list) != 5:
                            print('Error: value must have exactly 5 columns.')
                            continue
                        code = query.insert(value_int_list[0],value_int_list[1],value_int_list[2],value_int_list[3],value_int_list[4])
                        if code == False:
                            print('Error: insert false(same key exist in table already).')
                            continue
                        continue
            print("Usage: insert into Student (num,num,num,num,num);")
        ##select data
        elif inputs[0] == "select":
            if inputs[1][0] == '(' and inputs[1][-1] == ')' and inputs[2] == "from" and inputs[4]=='where' and len(inputs) == 6:
                column_selected = inputs[1][1:-1].split(',')
                if len(column_selected) != 5:
                    print('Error: value must have exactly 5 columns.')
                    continue
                column_selected = [int(i) for i in column_selected]
                if len(inputs[5]) > 2:
                    if '=' in inputs[5]:
                        temp = inputs[5].split('=')
                        if len(temp) == 2:
                            target_table = db.get_table(inputs[3])
                            if target_table == False:
                                print('Error: no such table exist.')
                                continue
                            query = Query(target_table)
                            record_list = query.select(int(temp[1]), int(temp[0]) , column_selected)
                            if not record_list:
                                print(record_list)
                                print('No data found')
                                continue
                            for rec in record_list:
                                print(rec.columns)
                            continue
            print("Usage: select (1,0,1,1,0) from Student where [column_number: only support 0]=99;")
        elif inputs[0] == "delete":
            if inputs[1] == 'from' and inputs[3] == 'where' and len(inputs) == 5:
                if len(inputs[4]) > 2:
                    if inputs[4][0:2] == '0=':
                        ##############
                        target_table = db.get_table(inputs[2])
                        if target_table == False:
                            print('Error: no such table exist.')
                            continue
                        query = Query(target_table)
                        ##############
                        code = query.delete(int(inputs[4][2:]))
                        if code == False:
                            print('Error: delete failed, no such data in table')
                        continue
            print("Usage: delete from table_name where 0=99;")
        elif inputs[0] == "update":
            if inputs[3] == 'where' and len(inputs) == 5:
                if len(inputs[4]) > 2:
                    if inputs[4][0:2] == '0=':
                        ##############
                        target_table = db.get_table(inputs[1])
                        if target_table == False:
                            print('Error: no such table exist.')
                            continue
                        query = Query(target_table)
                        ##############
                        if len(inputs[2]) > 2:
                            temp = inputs[2].split('=')
                            if len(temp) == 2:
                                updated_columns = [None, None, None, None, None]
                                updated_columns[int(temp[0])] = int(temp[1])
                                p_key = int(inputs[4][2:])
                                code = query.update(p_key, *updated_columns)
                                if code == False:
                                    print("Error: data not found, cannot update")
                                continue
            print("Usage: update Student [column_number]=10 where [column_number]=99;")
        elif inputs[0] == "sum":
            ##sum data
            print("fully tested in m1_tester.py.")

# "create table Student;" 
# "drop table Student"
# "insert into Student (num,num,num,num,num);"
# "select (1,0,1,1,0) from Student where [column_number]=99;"
# "delete from table where [column_number]=99;"
# "update Student [column_number]=10 where [column_number]=99;"
# "sum [column_number] from Student where [column_number]=99"

if __name__ == "__main__":
    main()

'''
show delete works:

create table a;
create index a 1;
insert into a (1,2,3,4,5);
insert into a (2,2,3,4,5);
select (1,1,1,1,1) from a where 0=1;
delete from a where 0=1;
select (1,1,1,1,1) from a where 0=1;


show update works:

create table a;
insert into a (1,2,3,4,5);
select (1,1,1,1,1) from a where 0=1;
update a 2=10 where 0=1;
select (1,1,1,1,1) from a where 0=1;
update a 3=20 where 0=1;
select (1,1,1,1,1) from a where 0=1;
update a 4=30 where 0=1;
select (1,1,1,1,1) from a where 0=1;





'''