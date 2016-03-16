create or replace PROCEDURE do_industry
IS

   v_member_windcode      wind.aindexmemberscitics.s_con_windcode%TYPE;
   v_indate               wind.aindexmemberscitics.s_con_indate%TYPE;
   v_outdate              wind.aindexmemberscitics.s_con_outdate%TYPE;
   v_industry_id          wind.aindexdescription.s_info_windcode%TYPE;
   v_industry_name        wind.aindexdescription.s_info_name%TYPE;
   v_id                   perfdata.portholding.id%TYPE; 
   v_l_date               perfdata.portholding.l_date%TYPE;              
   
   CURSOR c_industry_1 IS 
   SELECT  a.s_info_windcode industry_id, a.s_info_name industry_name,
           b.s_con_indate indate, b.s_con_outdate outdate
   FROM  wind.aindexdescription a, wind.aindexmemberscitics b
   WHERE a.s_info_windcode = b.s_info_windcode
     AND b.s_con_windcode=v_member_windcode
     AND b.s_con_indate <= v_l_date
     AND nvl(b.s_con_outdate,to_char(SYSDATE,'yyyymmdd'))>=v_l_date;
     
   CURSOR c_industry_2 IS
   SELECT industry_id, industry_name,
          indate,outdate
   FROM
   (SELECT  a.s_info_windcode industry_id, a.s_info_name industry_name,
           b.s_con_indate indate, b.s_con_outdate outdate
   FROM  wind.aindexdescription a, wind.aindexmemberscitics b
   WHERE a.s_info_windcode = b.s_info_windcode
     AND b.s_con_windcode=v_member_windcode
     AND b.s_con_indate >= v_l_date
   ORDER BY  b.s_con_indate)
   WHERE rownum = 1;
     
   CURSOR c_get_holding_stockcode IS
   SELECT a.id,a.l_date,a.wind_security_code
    FROM portholding a
   WHERE a.citics_industry_code IS NULL
     AND a.security_type='stock'
     AND a.data_status='1';
   
BEGIN
  
  OPEN c_get_holding_stockcode;
  LOOP 
    FETCH c_get_holding_stockcode
     INTO v_id, v_l_date,v_member_windcode;
    EXIT WHEN c_get_holding_stockcode%NOTFOUND;
       --find industry_id , name
       OPEN c_industry_1;
       FETCH c_industry_1
        INTO v_industry_id, 
             v_industry_name,
             v_indate,
             v_outdate;
       CLOSE c_industry_1; 
       IF v_industry_id IS NULL THEN
         OPEN c_industry_2;
         FETCH c_industry_2
          INTO  v_industry_id, 
                v_industry_name,
                v_indate,
                v_outdate;
         CLOSE c_industry_2;
       END IF;
       
       UPDATE portholding 
          SET citics_industry_code=v_industry_id,
              citics_industry_name=v_industry_name
        WHERE id=v_id;
                             
  END LOOP;
  CLOSE c_get_holding_stockcode;
END do_industry;
/
