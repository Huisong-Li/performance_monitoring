--Create table
create table perfdata.performance_statics 
(
  ID                          varchar2(40) not null,
  STATS_DATE                  date default sysdate, 
  FUND_ID                     varchar2(20),
  PORT_ID                     varchar2(20),
  START_DATE                  varchar2(8),
  END_DATE                    varchar2(8),
  INDICATOR_NAME              varchar2(20),
  CHINESE_NAME                varchar2(20),
  STATICS_VALUE               number(20,5),
  FCD                         date default sysdate,
  FCU                         varchar2(40),
  LCD                         date default sysdate,
  LCU                         varchar2(40),
  DATA_STATUS                 char(1) default '1'
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
--Create/Recreate primary, unique and foreign key constraint
alter table perfdata.performance_statics
  add constraint PK_pfStatics_ID primary key (ID)
  using index
  tablespace perfdata
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
  next 1M
  minextents 1
  maxextents unlimited
  )
  /
