--Create table
drop table portAsset_bak;
create table perfdata.portAsset_bak 
(
  ID                          varchar2(40),
  FUND_ID                     varchar2(20),
  PORT_ID                     varchar2(20),
  L_DATE                      varchar2(8),
  VALUES_COSTS                varchar2(8),
  DEPOSIT_ASSET               number(20,5),
  DEPOSIT_RESERVATION         number(20,5),
  DEPOSIT_RECOGNIZANCE        number(20,5),
  STOCK_ASSET                 number(20,5),
  BOND_ASSET                  number(20,5),
  ASSETS_BACKED_SECURITY      number(20,5),
  FUND_ASSET                  number(20,5),
  FINANCIAL_PRODUCTS          number(20,5),
  REPO_ASSET                  number(20,5),
  DIVIDEND_RECEIVABLE         number(20,5),
  INTEREST_RECEIVABLE         number(20,5),
  SUBSCRIPTION_RECEIVABLE     number(20,5),
  OTHER_RECEIVABLES           number(20,5),
  BAD_DEBT_RESERVES           number(20,5),
  LIQUIDATION_SECURITY        number(20,5),
  FUTURES_ASSET               number(20,5),
  ACCUMULATE_PROFIT           number(20,5),
  ALLOCATBLE_PROFIT           number(20,5),
  NET_ASSETS                  number(20,5),
  TOTAL_ASSETS                number(20,5),
  CREDIT_VALUE                number(20,5),
  ACCUMULATE_UNIT_VALUE       number(20,5),
  UNIT_VALUE_YESTERDAY        number(20,5),
  UNIT_VALUE                  number(20,5),
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
  )
/
