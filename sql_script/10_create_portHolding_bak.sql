--Create table
create table perfdata.portHolding_bak
(
  ID                            varchar2(40),
  FUND_ID                       varchar2(20),
  PORT_ID                       varchar2(20),
  L_DATE                        varchar2(8),
  ACCOUNT_ID                    varchar2(20),
  ACCOUNT_NAME                  varchar2(40),
  POSITION_FLAG                 varchar2(20),
  MARKET_NO                     varchar2(20),
  SUB_MARKET_NO                 varchar2(20),
  WIND_SECURITY_CODE            varchar2(20),
  SECURITY_CODE                 varchar2(20),
  SECURITY_TYPE                 varchar2(20),
  CITICS_INDUSTRY_CODE          varchar2(20),
  CITICS_INDUSTRY_NAME          varchar2(20),
  AMOUNT                        number(20,5),
  UNIT_COST                     number(20,5),
  TOTAL_COST                    number(20,5),
  MARKET_PRICE                  number(20,5),
  MARKET_VALUE                  number(20,5),
  PANDL                         number(20,5),
  NET_ASSET_PERCENT             number(20,5),
  NET_TOTAL_ASSET_PERCENT       number(20,5),
  BEGIN_AMOUNT                  number(20,5),
  BUY_AMOUNT                    number(20,5),
  SALE_AMOUNT                   number(20,5),
  BUY_CASH                      number(20,5),
  SALE_CASH                     number(20,5),
  BUY_FEE                       number(20,5),
  SALE_FEE                      number(20,5),
  FCD                           date,
  FCU                           varchar2(40),
  LCD                           date,
  LCU                           varchar2(40),
  DATA_STATUS                   char(1) default '1'
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
