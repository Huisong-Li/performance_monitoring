delete from portasset;
delete from performance_statics;
delete from portholding;
delete from portasset_bak;
delete from performance_statics_bak;
delete from portholding_bak;
drop sequence perfdata.S_portAsset;
drop sequence perfdata.S_performance_statics;
drop sequence perfdata.S_portHolding;
commit;