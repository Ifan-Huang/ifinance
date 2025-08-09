-- auto-generated definition
create table stock_daily_finance
(
    id        bigint unsigned auto_increment
        primary key,
    symbol    varchar(20)     null comment '代号',
    dates     int unsigned    null comment '日期',
    open      decimal(20, 2)  null comment '开盘价',
    high      decimal(20, 2)  null comment '最高价',
    low       decimal(20, 2)  null comment '最低价',
    close     decimal(20, 2)  null comment '收盘价',
    adj_close decimal(20, 2)  null comment '复权收盘价',
    volume    bigint unsigned null comment '成交量',
    source    varchar(50)     null comment '来源',
    created   datetime        null comment '创建时间',
    constraint uni_symbol_dates_source
        unique (symbol, dates, source)
);

create index idx_dates_symbol
    on stock_daily_finance (dates, symbol);

