create table if not exists cdm.deliveries(
id serial primary key,
courier_id text not null,
courier_name text not null,
settlement_year integer not null,
settlement_month integer check(settlement_month >=1 and settlement_month <=12) not null,
orders_count integer check(orders_count >=0) not null,
orders_total_sum integer check(orders_total_sum >=0) not null,
rate_avg numeric(14,2) not null,
order_processing_fee numeric(14,2) not null,
courier_order_sum numeric(14,2) not null, 
courier_tips_sum numeric(14,2) not null,
courier_reward_sum numeric(14,2) not null
);
