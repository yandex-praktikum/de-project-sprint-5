STG слой 
create table if not exists stg.restaurants(
id serial primary key,
restaurant_id text ,
restaurant_name text 
);

create table if not exists stg.couriers(
id serial primary key,
courier_id text   ,
courier_name text 
);

create table if not exists stg.deliveries(
id serial primary key,
order_id text ,
order_ts timestamp  ,
delivery_id text  ,
courier_id text  ,
address text  , 
delivery_ts timestamp  ,
rate integer  ,
"sum" integer  ,
tip_sum integer  
);
