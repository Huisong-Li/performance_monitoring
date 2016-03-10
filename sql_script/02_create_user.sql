create user perfdata
  identified by "perfdata123"
  default tablespace perfdata
  temporary tablespace TEMP
  profile DEFAULT;
-- Grant/Revoke system privileges 
grant connect, resource to perfdata with admin option;
--grant delete any table to perfdata;
--grant execute any procedure to perfdata;
--grant insert any table to perfdata;
--grant select any table to perfdata;
grant unlimited tablespace to perfdata;
grant update any table to perfdata;
grant create any sequence to perfdata;
