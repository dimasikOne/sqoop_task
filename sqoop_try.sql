Создаем таблицы и заполняем их данными:

create table employee(
    id int,
    salary int, 
    department_id int, 
    boss_id int
);

create table dep(
    id int,
    name VARCHAR2(100), 
    employee_count int
);


create table rep_card(
    total_days number,
    total_time number,
    total_days_off number,
    holidays_left number,
    employee_id number
);


create table task(
    past_tasks number,
    active_tasks number,
    future_tasks number,
    employee_id number
);

insert into employee (id,salary,department_id,boss_id) values (1,2,3,4);
insert into employee  values (5,6,7,8);

declare
  temp_id number;
  temp_salary number;
  temp_department_id number;
  temp_boss_id number;
  
  temp_employee_count number;
  temp_dep_name VARCHAR2(100);
  
  temp_days number;
  temp_time number;
  temp_days_off number;
  temp_holidays number;
  
  temp_past_tasks number;
  temp_active_tasks number;
  temp_future_tasks number;
  
  i number;
begin
  i := 1;
  while i <=1000000 loop
    temp_id := mod(DBMS_RANDOM.RANDOM, 1000000);
	temp_salary := mod(DBMS_RANDOM.RANDOM, 1000000);
    temp_department_id := mod(DBMS_RANDOM.RANDOM, 1000000);
    temp_boss_id := mod(DBMS_RANDOM.RANDOM, 1000000);
    temp_employee_count := mod(DBMS_RANDOM.RANDOM, 1000000);
    temp_days := mod(DBMS_RANDOM.RANDOM, 1000000);
    temp_time := mod(DBMS_RANDOM.RANDOM, 1000000);
    temp_days_off := mod(DBMS_RANDOM.RANDOM, 1000000);
    temp_holidays := mod(DBMS_RANDOM.RANDOM, 1000000);
    temp_past_tasks := mod(DBMS_RANDOM.RANDOM, 1000000);
    temp_active_tasks := mod(DBMS_RANDOM.RANDOM, 1000000);
    temp_future_tasks := mod(DBMS_RANDOM.RANDOM, 1000000);
    
    
     if mod(temp_department_id,5)=2 then
        temp_dep_name:='аналитики';
    end if;
    if mod(temp_department_id,5)=1 then
        temp_dep_name:='it-отдел';
    end if;
        if mod(temp_department_id,5)=3 then
        temp_dep_name:='отдел кадров';
    end if;
    
    if mod(temp_department_id,5)=4 then
        temp_dep_name:='секретариат';
    end if;
    
    if mod(temp_department_id,5)=0 then
        temp_dep_name:='служба поддержки';
    end if;
   
   if temp_id<0 then
        temp_id:= temp_id*(-1);
    end if;
    
    if temp_salary<0 then
        temp_salary:= temp_salary*(-1);
    end if;
    
    if temp_department_id<0 then
        temp_department_id:= temp_department_id*(-1);
    end if;
    
    if temp_boss_id<0 then
        temp_boss_id:= temp_boss_id*(-1);
    end if;
    
     if temp_employee_count<0 then
        temp_employee_count:= temp_employee_count*(-1);
    end if;
    
     if temp_days<0 then
        temp_days:= temp_days*(-1);
    end if;
    
     if temp_days_off<0 then
        temp_days_off:= temp_days_off*(-1);
    end if;
    
     if temp_holidays<0 then
        temp_holidays:= temp_holidays*(-1);
    end if;
    
     if temp_past_tasks<0 then
        temp_past_tasks:= temp_past_tasks*(-1);
    end if;
    
     if temp_active_tasks<0 then
        temp_active_tasks:= temp_active_tasks*(-1);
    end if;
    
     if temp_future_tasks<0 then
        temp_future_tasks:= temp_future_tasks*(-1);
    end if;
    
     if temp_time<0 then
        temp_time:= temp_time*(-1);
    end if;
    
    insert into employee values (temp_id, temp_salary, temp_department_id, temp_boss_id);
    insert into dep values (temp_department_id, temp_dep_name, temp_employee_count);
    insert into rep_card values (temp_days, temp_time, temp_days_off, temp_holidays, temp_id);
    insert into task values (temp_past_tasks, temp_active_tasks, temp_future_tasks, temp_id);
    
	
    i:=i+1;
    
end loop;
  commit;
end;

Заливаем в hive, используя sqoop:
 
sqoop import --connect jdbc:oracle:thin:@192.168.88.92:1521:orcl \
--username test_user --password test_user \
--query "SELECT * FROM employee WHERE \$CONDITIONS" \
--target-dir /user/usertest/employee2 \
--hive-import --create-hive-table --hive-table "employee2" -m 1

sqoop import --connect jdbc:oracle:thin:@192.168.88.92:1521:orcl \
--username test_user --password test_user \
--query "SELECT * FROM dep WHERE \$CONDITIONS" \
--target-dir /user/usertest/dep2 \
--hive-import --create-hive-table --hive-table "dep2" -m 1

sqoop import --connect jdbc:oracle:thin:@192.168.88.92:1521:orcl \
--username test_user --password test_user \
--query "SELECT * FROM rep_card WHERE \$CONDITIONS" \
--target-dir /user/usertest/rep_card2 \
--hive-import --create-hive-table --hive-table "rep_card2" -m 1

sqoop import --connect jdbc:oracle:thin:@192.168.88.92:1521:orcl \
--username test_user --password test_user \
--query "SELECT * FROM task WHERE \$CONDITIONS" \
--target-dir /user/usertest/task2 \
--hive-import --create-hive-table --hive-table "task2" -m 1

Делаем таблички orc и parquet:

create table task_orc stored as orc as select * from task
create table rep_card_orc stored as orc as select * from rep_card


create table task_parquet stored as parquet as select * from task
create table rep_card_parquet stored as parquet as select * from rep_card

Запросы:
1.)
select r.total_days, r.total_time, t.past_tasks from rep_card r, task t 
where r.employee_id=t.employee_id;

select r.total_days, r.total_time, t.past_tasks from rep_card_orc r, task_orc t 
where r.employee_id=t.employee_id;

select r.total_days, r.total_time, t.past_tasks from rep_card_parquet r, task_parquet t 
where r.employee_id=t.employee_id;

2.)
select t.max(active_tasks), r.holidays_left, r.employee_id from rep_card r, task t
where r.employee_id=t.employee_id 

select t.max(active_tasks), r.holidays_left, r.employee_id from rep_card_orc r, task_orc t
where r.employee_id=t.employee_id

select t.max(active_tasks), r.holidays_left, r.employee_id from rep_card_parquet r, task_parquet t
where r.employee_id=t.employee_id