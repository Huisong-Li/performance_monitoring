--Create table
create table perfdata.performance_statics_bak 
(
  ID                          varchar2(40),
  STATS_DATE                  date, 
  FUND_ID                     varchar2(20),
  PORT_ID                     varchar2(20),
  START_DATE                  varchar2(8),
  END_DATE                    varchar2(8),
  INDICATOR_NAME              varchar2(20),
  CHINESE_NAME                varchar2(20),
  STATICS_VALUE               number(20,5),
  FCD                         date,
  FCU                         varchar2(40),
  LCD                         date,
  LCU                         varchar2(40),
  DATA_STATUS                 char(1)
)
tablespace perfdata
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
  next 1M
  minextents 1
  maxextents unlimited
  );