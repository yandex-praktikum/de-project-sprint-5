--2. Спроектируйте структуру таблиц для слоёв в хранилище

--2.1. Спроектируйте структуру витрины расчётов с курьерами

create table cdm.couriers_mart
(id 		          serial        primary key,
 courier_id           int           not null,
 courier_name         varchar(50)   not null,
 settlement_year      int           not null,
 settlement_month     int           not null,
 orders_count         int           not null,
 orders_total_sum     decimal(14,2) not null,
 rate_avg             decimal(1, 2),
 order_processing_fee decimal(14,2) not null,
 courier_order_sum    decimal(14,2) not null,
 courier_tips_sum     decimal(14,2) not null,
 courier_reward_sum   decimal(14,2) not null)

--2.2. Спроектируйте структуру DDS-слоя
 --Создадим таблицу с курьерами
 create table dds.dm_couriers
 (id           serial  primary key,
  courier_id   varchar not null,
  courier_name varchar not null);
 
 --Создадим таблицу с доставками
 create table dds.dm_deliveries
(id          serial        primary key,
 delivery_id varchar       not null,
 courier_id  int           not null,
 order_id    int           not null,
 address     text          not null,
 delivery_ts timestamp     not null,
 rate        smallint,
 order_sum   decimal(14,2) not null,
 tip_sum     decimal(14,2),
                           constraint couriers_fkey foreign key(courier_id) references dds.dm_couriers
 );

--Свяжем доставки и заказы
 alter table dds.dm_orders
 add column delivery_id int; 
 alter table dds.dm_orders
 add constraint deliveries_fkey foreign key (delivery_id) references dds.dm_deliveries;

--2.3. Спроектируйте структуру STG-слоя
--Создадим raw-таблицу для курьеров
create table stg.deliverysystem_couriers
(id   serial primary key,
 json text   not null);

--Создадим таблицу для хранения оффсета по курьерам
create table stg.couriers_offset
(offset_data    int,
 triggered_time timestamp);

--Создадим raw-таблицу для доставок
create table stg.deliverysystem_deliveries
(id   serial primary key,
 json text   not null);

--Создадим таблицу для хранения оффсета по доставкам
create table stg.deliveries_offset
(offset_data int,
 triggered_time timestamp);
