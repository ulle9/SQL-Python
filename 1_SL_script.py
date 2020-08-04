#!/usr/bin/env python
# coding: utf-8

import os
import cx_Oracle
import xlsxwriter
import re
import numpy
import pandas as pd

# init of oracle client
try:
    cx_Oracle.init_oracle_client(lib_dir=r"---")
except cx_Oracle.ProgrammingError:
    pass
    
# SQL-queries
query_common = """SELECT /*+ parallel*/ -- о смерти
 --       lpu.CODE_LPU,                            		        	-- Код ЛПУ
 --   cf.AGENT,
      	lpu.FULLNAME,							-- Наименование ЛПУ
        cf.C_NUM_CHAR ,                          		       	    -- Номер свидетельства
        TO_CHAR(cf.DATE_OUT,'DD.MM.YYYY'),                      -- Дата выдачи
        cf.STATUS,                               		       	    -- Условие выдачи (окончательное, предварительное...)
        decode (cf.SEX,0,2,1),                   		          	-- Пол
        TO_CHAR(cf.BIRTHDATE,'DD.MM.YYYY') ,                    -- Дата рождения
        TO_CHAR(cf.DATE_DEATH,'DD.MM.YYYY') ,                   -- Дата смерти
        cf.FULL_YEARS,                                          -- Возраст
        cf.SURNAME	,					        	                        -- Фамилия
        cf.FIRSTNAME  ,                          		         	  -- Имя
        cf.LASTNAME  ,                           		      		  -- Отчество
        dc.DIR_PLACE,                          		     		    -- Место смерти

        MAX(CASE cc.CODE WHEN 1 THEN cc.DISEASE ELSE null end) as PRICH_1,	       	-- Причина смерти А
        MAX(CASE cc.CODE WHEN 1 THEN cc.PERIOD ELSE null end) as PERIOD_1,       		-- Период до смерти А
        MAX(CASE cc.CODE WHEN 1 THEN cc.MKB ELSE null end) as MKB10A, 	        	-- Код МКБ-10 причины смерти А
        MAX(CASE cc.CODE WHEN 2 THEN cc.DISEASE ELSE null end) as PRICH_2, 	
        MAX(CASE cc.CODE WHEN 2 THEN cc.PERIOD ELSE null end) as PERIOD_2, 	
        MAX(CASE cc.CODE WHEN 2 THEN cc.MKB ELSE null end) as MKB10B,	
        MAX(CASE cc.CODE WHEN 3 THEN cc.DISEASE ELSE null end) as PRICH_3,	
        MAX(CASE cc.CODE WHEN 3 THEN cc.PERIOD ELSE null end) as PERIOD_3,	
        MAX(CASE cc.CODE WHEN 3 THEN cc.MKB ELSE null END) as MKB10C,  	
        MAX(CASE cc.CODE WHEN 4 THEN cc.DISEASE ELSE null end) as PRICH_4,	
        MAX(CASE cc.CODE WHEN 4 THEN cc.PERIOD ELSE null end) as PERIOD_4, 	
        MAX(CASE cc.CODE WHEN 4 THEN cc.MKB ELSE null end) as MKB10D,	
	      MAX(CASE cc.CODE WHEN 5 THEN cc.DISEASE ELSE null end)as PRICH_5, 	
        MAX(CASE cc.CODE WHEN 5 THEN cc.PERIOD ELSE null end) as PERIOD_5,	
        MAX(CASE cc.CODE WHEN 5 THEN cc.MKB ELSE null end)as MKB10E,
        d_pkg_agent_addrs.GET_ACTUAL_ON_DATE(cf.AGENT,cf.DATE_DEATH,0,'SHORT'),                -- Место жительства прописка
        d_pkg_agent_addrs.GET_ACTUAL_ON_DATE(cf.AGENT,cf.DATE_DEATH,1,'FULL') ,                -- проживание
        max(ar.LPU_REG_NAME)
      --  ap.P_NUM,
      --  rzn.KLPU
        --  t1.MKB
        
FROM
	USR39_V_CF_ANALYTICS cf 
  LEFT JOIN D_V_CF_DEATH_CAUSES cc ON cc.PID = cf.ID   
  LEFT JOIN D_V_CF_DEATH_CONTENTS dc ON cf.ID = dc.PID
	LEFT JOIN D_LPU lpu ON lpu.id = cf.LPU
  LEFT JOIN D_V_AGENT_REGISTRATION ar ON ar.PID = cf.AGENT AND (ar.END_DATE is NULL OR (TRUNC(ar.END_DATE) BETWEEN TRUNC(cf.DATE_DEATH-5) AND sysdate)) AND ar.BEGIN_DATE < cf.DATE_DEATH AND ar.REGISTER_PURPOSE_ID in (100961856, 100961855,113772191)

WHERE
/*	TRUNC(cf.DATE_OUT) >= TRUNC(to_date ('01.01.2018','DD.MM.YYYY'))					-- Дата выдачи свидетельства
	AND TRUNC(cf.DATE_OUT) <= TRUNC(to_date ('31.01.2018','DD.MM.YYYY'))	*/			-- Дата выдачи свидетельства
  TRUNC(cf.DATE_DEATH) >= {startdate}					-- Дата смерти
	AND TRUNC(cf.DATE_DEATH) <= {finishdate}				-- Дата смерти*/
--	 (aa.BEGIN_DATE <= cf.DATE_OUT or aa.BEGIN_DATE is null)		-- Дата регистрации пациента актуальная на дату выдачи свидетельства
--	AND (aa.END_DATE  >= cf.DATE_OUT or aa.END_DATE is null)
--	AND aa.IS_REAL = 1 
	AND cf.C_KIND = 2							-- Вид журнала 2 - о смерти, 3 - перинатальная смерть
  AND EXISTS (select null from D_V_CF_DEATH_CAUSES cc1 
                               where cc1.PID = cf.ID 
                               and cc1.CODE in (1,2,3)
                            AND {mkb}
                            )
{hospital}         -- Место смерти 3 - стационар

{female}   --Женщины 
{male}  --Мужчин 

GROUP BY
--	lpu.CODE_LPU,                            		        	-- Код ЛПУ
      --  cf.AGENT,
      	lpu.FULLNAME,							-- Наименование ЛПУ
        cf.C_NUM_CHAR ,                          		       	-- Номер свидетельства
        cf.DATE_OUT,                             		         	-- Дата выдачи
        cf.STATUS,                               		       	-- Условие выдачи (окончательное, предварительное...)
        decode (cf.SEX,0,2,1),                   		          	-- Пол
        cf.BIRTHDATE ,                           		           	-- Дата рождения
        cf.DATE_DEATH ,                         		           	-- Дата смерти
        cf.FULL_YEARS,
        cf.SURNAME	,					        	-- Фамилия
        cf.FIRSTNAME  ,                          		         	-- Имя
        cf.LASTNAME  ,                           		      		-- Отчество
        dc.DIR_PLACE  , 
        
        d_pkg_agent_addrs.GET_ACTUAL_ON_DATE(cf.AGENT,cf.DATE_DEATH,0,'SHORT'),
        d_pkg_agent_addrs.GET_ACTUAL_ON_DATE(cf.AGENT,cf.DATE_DEATH,1,'FULL'),
       ar.LPU_REG_NAME
      --  ap.P_NUM,
      --  rzn.KLPU


ORDER BY lpu.FULLNAME"""

query_hosp_20_24 = """SELECT /*+ parallel*/ -- о смерти
 --       lpu.CODE_LPU,                            		        	-- Код ЛПУ
 --   cf.AGENT,
      	lpu.FULLNAME,							-- Наименование ЛПУ
        cf.C_NUM_CHAR ,                          		       	    -- Номер свидетельства
        TO_CHAR(cf.DATE_OUT,'DD.MM.YYYY'),                      -- Дата выдачи
        cf.STATUS,                               		       	    -- Условие выдачи (окончательное, предварительное...)
        decode (cf.SEX,0,2,1),                   		          	-- Пол
        TO_CHAR(cf.BIRTHDATE,'DD.MM.YYYY') ,                    -- Дата рождения
        TO_CHAR(cf.DATE_DEATH,'DD.MM.YYYY') ,                   -- Дата смерти
        cf.FULL_YEARS,                                          -- Возраст
        cf.SURNAME	,					        	                        -- Фамилия
        cf.FIRSTNAME  ,                          		         	  -- Имя
        cf.LASTNAME  ,                           		      		  -- Отчество
        dc.DIR_PLACE,                          		     		    -- Место смерти

        MAX(CASE cc.CODE WHEN 1 THEN cc.DISEASE ELSE null end) as PRICH_1,	       	-- Причина смерти А
        MAX(CASE cc.CODE WHEN 1 THEN cc.PERIOD ELSE null end) as PERIOD_1,       		-- Период до смерти А
        MAX(CASE cc.CODE WHEN 1 THEN cc.MKB ELSE null end) as MKB10A, 	        	-- Код МКБ-10 причины смерти А
        MAX(CASE cc.CODE WHEN 2 THEN cc.DISEASE ELSE null end) as PRICH_2, 	
        MAX(CASE cc.CODE WHEN 2 THEN cc.PERIOD ELSE null end) as PERIOD_2, 	
        MAX(CASE cc.CODE WHEN 2 THEN cc.MKB ELSE null end) as MKB10B,	
        MAX(CASE cc.CODE WHEN 3 THEN cc.DISEASE ELSE null end) as PRICH_3,	
        MAX(CASE cc.CODE WHEN 3 THEN cc.PERIOD ELSE null end) as PERIOD_3,	
        MAX(CASE cc.CODE WHEN 3 THEN cc.MKB ELSE null END) as MKB10C,  	
        MAX(CASE cc.CODE WHEN 4 THEN cc.DISEASE ELSE null end) as PRICH_4,	
        MAX(CASE cc.CODE WHEN 4 THEN cc.PERIOD ELSE null end) as PERIOD_4, 	
        MAX(CASE cc.CODE WHEN 4 THEN cc.MKB ELSE null end) as MKB10D,	
	      MAX(CASE cc.CODE WHEN 5 THEN cc.DISEASE ELSE null end)as PRICH_5, 	
        MAX(CASE cc.CODE WHEN 5 THEN cc.PERIOD ELSE null end) as PERIOD_5,	
        MAX(CASE cc.CODE WHEN 5 THEN cc.MKB ELSE null end)as MKB10E,
        d_pkg_agent_addrs.GET_ACTUAL_ON_DATE(cf.AGENT,cf.DATE_DEATH,0,'SHORT'),                -- Место жительства прописка
        d_pkg_agent_addrs.GET_ACTUAL_ON_DATE(cf.AGENT,cf.DATE_DEATH,1,'FULL') ,                -- проживание
        max(ar.LPU_REG_NAME),

        lpu1.FULLNAME -- |модификация по исходам госпитализации в архивах ИБ|
FROM
	USR39_V_CF_ANALYTICS cf 
  LEFT JOIN D_V_CF_DEATH_CAUSES cc ON cc.PID = cf.ID   
  LEFT JOIN D_V_CF_DEATH_CONTENTS dc ON cf.ID = dc.PID
	LEFT JOIN D_LPU lpu ON lpu.id = cf.LPU
  LEFT JOIN D_V_AGENT_REGISTRATION ar ON ar.PID = cf.AGENT AND (ar.END_DATE is NULL OR (TRUNC(ar.END_DATE) BETWEEN TRUNC(cf.DATE_DEATH-5) AND sysdate)) AND ar.BEGIN_DATE < cf.DATE_DEATH AND ar.REGISTER_PURPOSE_ID in (100961856, 100961855,113772191)
  
    -- |модификация по исходам госпитализации в архивах ИБ|
  LEFT JOIN (
            SELECT dps.AGENT, dhh.PATIENT, dhh.LPU FROM D_PERSMEDCARD dps LEFT JOIN D_HOSP_HISTORIES dhh ON dps.ID = dhh.PATIENT
            WHERE HOSP_RESULT = 92094160
            ) dhh1 ON cf.AGENT = dhh1.AGENT
  LEFT JOIN D_LPU lpu1 ON dhh1.LPU = lpu1.id
    -- |модификация по исходам госпитализации в архивах ИБ|
WHERE
/*	TRUNC(cf.DATE_OUT) >= TRUNC(to_date ('01.01.2018','DD.MM.YYYY'))					-- Дата выдачи свидетельства
	AND TRUNC(cf.DATE_OUT) <= TRUNC(to_date ('31.01.2018','DD.MM.YYYY'))	*/			-- Дата выдачи свидетельства
  TRUNC(cf.DATE_DEATH) >= {startdate}					-- Дата смерти
	AND TRUNC(cf.DATE_DEATH) <= {finishdate}				-- Дата смерти*/
--	 (aa.BEGIN_DATE <= cf.DATE_OUT or aa.BEGIN_DATE is null)		-- Дата регистрации пациента актуальная на дату выдачи свидетельства
--	AND (aa.END_DATE  >= cf.DATE_OUT or aa.END_DATE is null)
--	AND aa.IS_REAL = 1 
 
	AND cf.C_KIND = 2							-- Вид журнала 2 - о смерти, 3 - перинатальная смерть
  AND EXISTS (select null from D_V_CF_DEATH_CAUSES cc1 
                               where cc1.PID = cf.ID 
                               and cc1.CODE in (1,2,3)
                                AND {mkb} 

           )
{hospital}         -- Место смерти 3 - стационар

{female}   --Женщины 
{male}  --Мужчин 

GROUP BY
--	lpu.CODE_LPU,                            		        	-- Код ЛПУ
      --  cf.AGENT,
      	lpu.FULLNAME,							-- Наименование ЛПУ
        cf.C_NUM_CHAR ,                          		       	-- Номер свидетельства
        cf.DATE_OUT,                             		         	-- Дата выдачи
        cf.STATUS,                               		       	-- Условие выдачи (окончательное, предварительное...)
        decode (cf.SEX,0,2,1),                   		          	-- Пол
        cf.BIRTHDATE ,                           		           	-- Дата рождения
        cf.DATE_DEATH ,                         		           	-- Дата смерти
        cf.FULL_YEARS,
        cf.SURNAME	,					        	-- Фамилия
        cf.FIRSTNAME  ,                          		         	-- Имя
        cf.LASTNAME  ,                           		      		-- Отчество
        dc.DIR_PLACE  , 
        
        d_pkg_agent_addrs.GET_ACTUAL_ON_DATE(cf.AGENT,cf.DATE_DEATH,0,'SHORT'),
        d_pkg_agent_addrs.GET_ACTUAL_ON_DATE(cf.AGENT,cf.DATE_DEATH,1,'FULL'),
       ar.LPU_REG_NAME,
       
       lpu1.FULLNAME -- |модификация по исходам госпитализации в архивах ИБ|


ORDER BY lpu.FULLNAME"""


class Savelives():
    """Class to export Saving lives statistical data from MIS to excel file format (basic class without GUI)"""

    # base variables
    _hospital = "AND dc.DIR_PLACE = '3'"
    _startdate = "TO_DATE(TRUNC(SYSDATE,'YYYY'))"
    _finishdate = "TO_DATE(TRUNC(SYSDATE, 'MM')-1)"
    _male = '''AND d_pkg_dat_tools.FULL_YEARS(cf.DATE_DEATH,cf.BIRTHDATE) < 60
            AND d_pkg_dat_tools.FULL_YEARS(cf.DATE_DEATH,cf.BIRTHDATE) >= 16
            AND cf.SEX = 1'''
    _female = '''AND d_pkg_dat_tools.FULL_YEARS(cf.DATE_DEATH,cf.BIRTHDATE) < 55
            AND d_pkg_dat_tools.FULL_YEARS(cf.DATE_DEATH,cf.BIRTHDATE) >= 16 
            AND cf.SEX = 0'''
    
    _mkbvars_hosp = {"I20_stac_pd": "cc1.MKB like 'I20.0'",
        "I21_stac_pd": "cc1.MKB like 'I21%'",
        "I22_stac_pd": "cc1.MKB like 'I22%'",
        "I24_stac_pd": "cc1.MKB like 'I24%'",
        "I60-I66_stac_pd": "((cc1.MKB BETWEEN 'I60%' AND 'I67') AND cc1.MKB != 'I67')",
        "I63,I65,I66_stac_pd": "(cc1.MKB like 'I63%' OR cc.MKB like 'I65%' OR cc.MKB like 'I66%')",
        "I60,I61,I62,I64_stac_pd": "(cc1.MKB like 'I60%' OR cc1.MKB like 'I61%' OR cc1.MKB like 'I62%' OR cc1.MKB like 'I64%')",
        "J00-J98_stac_pd": "((cc1.MKB BETWEEN 'J00%' AND 'J99') AND cc1.MKB != 'J99')",
        "J12-J16,J18_stac_pd": "(((cc1.MKB BETWEEN 'J12%' AND 'J17') AND cc1.MKB != 'J17') OR cc1.MKB like 'J18%')",
        "J44,J47_stac_pd": "(cc1.MKB like 'J44%' OR cc1.MKB like 'J47%')",
        "J45,J46_stac_pd": "(cc1.MKB like 'J45%' OR cc1.MKB like 'J46%')",
        "K00-K92_stac_pd": "((cc1.MKB BETWEEN 'K00%' AND 'K93') AND cc1.MKB != 'K93')",
        "K25-K26_stac_pd": "((cc1.MKB BETWEEN 'K25%' AND 'K27') AND cc1.MKB != 'K27')",
        "K70-K76_stac_pd": "((cc1.MKB BETWEEN 'K70%' AND 'K77') AND cc1.MKB != 'K77')",
        "K85-K86_stac_pd": "((cc1.MKB BETWEEN 'K85%' AND 'K87') AND cc1.MKB != 'K87')"}
    
    _mkbvars_trud = {"C00-C98_trud_pd": "((cc1.MKB BETWEEN 'C00%' AND 'C98') AND cc1.MKB != 'C98')",
            "C44_trud_pd": "cc1.MKB like 'C44%'",
            "I20-I25_trud_pd": "((cc1.MKB BETWEEN 'I20%' AND 'I26') AND cc1.MKB != 'I26')", 
            "I21_stac_pd": "cc1.MKB like 'I21%'",
            "I22_stac_pd": "cc1.MKB like 'I22%'",
            "I24_stac_pd": "cc1.MKB like 'I24%'",
            "I60-I66_stac_pd": "((cc1.MKB BETWEEN 'I60%' AND 'I67') AND cc1.MKB != 'I67')",
            "I60-I70_trud_pd": "((cc1.MKB BETWEEN 'I60%' AND 'I70') AND cc1.MKB != 'I70')",
            "I63,I65,I66_stac_pd": "(cc1.MKB like 'I63%' OR cc.MKB like 'I65%' OR cc.MKB like 'I66%')",
            "I60,I61,I62,I64_stac_pd": "(cc1.MKB like 'I60%' OR cc1.MKB like 'I61%' OR cc1.MKB like 'I62%' OR cc1.MKB like 'I64%')",
            "J12-J16,J18_stac_pd": "(((cc1.MKB BETWEEN 'J12%' AND 'J17') AND cc1.MKB != 'J17') OR cc1.MKB like 'J18%')",
            "J44,J47_stac_pd": "(cc1.MKB like 'J44%' OR cc1.MKB like 'J47%')",
            "J45,J46_stac_pd": "(cc1.MKB like 'J45%' OR cc1.MKB like 'J46%')",
            "K00-K92_stac_pd": "((cc1.MKB BETWEEN 'K00%' AND 'K93') AND cc1.MKB != 'K93')",
            "K25-K26_stac_pd": "((cc1.MKB BETWEEN 'K25%' AND 'K27') AND cc1.MKB != 'K27')",
            "K70-K76_stac_pd": "((cc1.MKB BETWEEN 'K70%' AND 'K77') AND cc1.MKB != 'K77')",
            "K85-K86_stac_pd": "((cc1.MKB BETWEEN 'K85%' AND 'K87') AND cc1.MKB != 'K87')"}
    
    names = (('ЛПУ', 'Номер', 'Дата выдачи', 'Статус', 'Пол', 'ДР', 'ДС', 'Возраст', 'Фамилия', 'Имя', 'Отчество', 'Место', 'Причина А',
             'Период А', 'МКБ А', 'Причина Б', 'Период Б', 'МКБ Б', 'Причина В', 'Период В', 'МКБ В', 'Причина Г', 'Период Г', 'МКБ Г',
             'Причина проч.', 'Период проч.', 'МКБ Проч.', 'Адрес регистрации', 'Адрес проживания (фактический)', 'ЛПУ обслуживания', 'ЛПУ смерти (по исходам ИБ)'),
             ('ЛПУ', 'Номер', 'Дата выдачи', 'Статус', 'Пол', 'ДР', 'ДС', 'Возраст', 'Фамилия', 'Имя', 'Отчество', 'Место', 'Причина А',
             'Период А', 'МКБ А', 'Причина Б', 'Период Б', 'МКБ Б', 'Причина В', 'Период В', 'МКБ В', 'Причина Г', 'Период Г', 'МКБ Г',
             'Причина проч.', 'Период проч.', 'МКБ Проч.', 'Адрес регистрации', 'Адрес проживания (фактический)', 'ЛПУ обслуживания'))
    
    mkbvars = False
    newquery = False
    
    _default_connect = ('---')
    
    # create connection to Oracle DB  
    def __init__(self, 
                 hospital=_hospital, 
                 startdate=_startdate, 
                 finishdate=_finishdate,
                 myconnect=_default_connect,
                 male=_male,
                 female=_female,
                 newquery=newquery,
                 mkbvars=mkbvars,
                 mkbvars_hosp=_mkbvars_hosp,
                 mkbvars_trud=_mkbvars_trud,
                 names=names):
        
            # initialisation
            try: 
                myconnection = cx_Oracle.connect(myconnect)
                mycursor = myconnection.cursor()
                
                self._myconnection = myconnection
                self.hospital = hospital
                self.startdate = startdate
                self.finishdate = finishdate
                self.mkbvars = mkbvars
                self._mkbvars_hosp = mkbvars_hosp
                self._mkbvars_trud = mkbvars_trud
                self.names = names
                self.newquery = newquery
                self._myconnection = myconnection
                self._male = male
                self._female = female
                self._query_common = query_common
                self._query_hosp_20_24 = query_hosp_20_24

                print("Connection to server is ready.")
            
            except cx_Oracle.DatabaseError:
                print("Error: Unable to connect to database: {}".format(myconnect))
    
    # function to get query result
    def read_query(self, 
               connection, 
               query, 
               names=None, 
               mkb=None, 
               hospital='',
               male = '',
               female = ''):
        
        mycursor = connection.cursor()
        try:
            if mkb:
                mycursor.execute(query.format(mkb=mkb, hospital=hospital, startdate=self.startdate, 
                                              finishdate=self.finishdate, male=male, female=female))
                rows = mycursor.fetchall()
                if not names: 
                    names = [x[0] for x in mycursor.description]
                return pd.DataFrame(rows, columns=names)

        finally:
            if mycursor is not None:
                mycursor.close()

    #Make directory, return path
    def makedir(self, path): 
        print("Текущий путь для сохранения файлов: {}".format(path))
        path = input('Введите новый путь для сохранения файлов или нажмите Enter для использования текущего пути: ') or path
        try:
            os.mkdir(path)
        except:
            pass
        finally:
            print("Каталог для сохранения файлов: {}".format(path))
        return path
        
    # iterator of hospital queries    
    def formhospital(self): 
        default_path = os.path.dirname(os.path.realpath(__file__))+"\\hospstat"
        path = self.makedir(default_path)
                
        for i,j in self._mkbvars_hosp.items():
            if ("I20" in j) or ("I21" in j) or ("I22" in j) or ("I24" in j):
                names = self.names[0]
                query = self._query_hosp_20_24
            else: 
                names = self.names[1]
                query = self._query_common
            table = self.read_query(self._myconnection, query, names, j, hospital=self.hospital)
            table.to_excel(r'{}\{}.xls'.format(path, i), index=False)
            print(i + ' is ready!')
            
    # iterator of labour queries 
    def formlabor(self): 
        default_path = os.path.dirname(os.path.realpath(__file__))+"\\trudstat"
        path = self.makedir(default_path)
            
        for i,j in self._mkbvars_trud.items():

            names = self.names[1]
            query = self._query_common
                
            table1 = self.read_query(self._myconnection, query, names, j, male=self._male)
            table2 = self.read_query(self._myconnection, query, names, j, female=self._female)

            writer = pd.ExcelWriter(r'{}\{}.xlsx'.format(path, i), 
                                    engine='xlsxwriter')
            table1.to_excel(writer, 'мужчины', index = False)
            table2.to_excel(writer, 'женщины', index = False)
            writer.save()
            print(i + ' is ready!')
    
    def set_startdate(self, newdate=False):
        if newdate and re.match(r'(0[1-9]|[12][0-9]|3[01])[-.](0[1-9]|1[012])[-.](20)\d\d', newdate):
            self.startdate = "TRUNC(to_date('{}','DD.MM.YYYY'))".format(newdate)
        else:
            print ("Ошибка. Необходимо указать дату в формате: 'DD.MM.YYYY'")             

    def set_finishdate(self, newdate=False):
        if newdate and re.match(r'(0[1-9]|[12][0-9]|3[01])[-.](0[1-9]|1[012])[-.](20)\d\d', newdate):
            self.finishdate = "TRUNC(to_date('{}','DD.MM.YYYY'))".format(newdate)
        else:
            print ("Ошибка. Необходимо указать дату в формате: 'DD.MM.YYYY'")
            
    def show_period(self):
        print(self.startdate)
        print(self.finishdate)
        
    def closecon(self):
        self._myconnection.close()
        print("Connection closed.")


inp = input("Работа с основным сервером? (y/n): ")
if inp == 'y':
    gen = Savelives(myconnect=('---'))
    print ("Режим работы: основной сервер")
else:
    gen = Savelives()
    print ("Режим работы: тестовый сервер")
   

h = input('Вы хотите изменить стандартные настройки периода формирования? (y/n)')
if h == 'y':
    ts = gen.set_startdate(input("Введите дату начала периода в формате: 'DD.MM.YYYY': "))
    tf = gen.set_finishdate(input("Введите дату конца периода в формате: 'DD.MM.YYYY': "))
    print("Период формирования:")
    gen.show_period()
    
h = input('Сформировать отчёты по стационару? (y/n)')
if h=='y':
    gen.formhospital()

h = input('Сформировать отчёты по трудоспособному населению? (y/n)')
if h=='y':
    gen.formlabor()

gen.closecon()
