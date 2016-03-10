CREATE TABLESPACE perfdata 
    NOLOGGING 
    DATAFILE 'd:\oracle\oradata\orcl\perfdata.dbf' SIZE 10M 
    REUSE AUTOEXTEND 
    ON NEXT  1280K EXTENT MANAGEMENT LOCAL 
    SEGMENT SPACE MANAGEMENT  AUTO ;
