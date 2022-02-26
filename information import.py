# -*- coding: utf-8 -*-
"""
Created on Wed Jul 14 15:00:36 2021

@author: SYSU-ESE-HF
"""

# -*- coding: utf-8 -*-
"""
整体思路是用pandas读入数据，然后依次导入每一列的节点、关系
"""
import pandas as pd
from py2neo import Graph, Node, Relationship, NodeMatcher
"""
读数据
"""

df = pd.read_excel('地块信息20210723.xlsx')
df_siteInformation = pd.read_excel('地块信息20210723.xlsx',sheet_name=0)
df_EmiitedPollutant = pd.read_excel('地块信息20210723.xlsx',sheet_name=1)
df_SitePollutants = pd.read_excel('地块信息20210723.xlsx',sheet_name=2)
df_RiskScore = pd.read_excel('地块信息20210723.xlsx',sheet_name=3)
df1=df_siteInformation
df2=df_EmiitedPollutant
df3=df_SitePollutants
df4=df_RiskScore
"""
删除列
"""
drop_col =['地块名称']

df1 = df.drop(drop_col, axis=1)

# 连接Neo4j服务
graph = Graph("bolt://localhost:7687", auth=('neo4j', '1234'))
matcher = NodeMatcher(graph)

print('Importing Sites Infomation......')
for i in range(len(df1)):
#    print(i)
    if str(df1['SiteName'][i])!='nan':
        name  = str(df1['SiteName'][i])
    else:
        name = ''
    if str(df1['SiteID'][i])!='nan':
        SiteID  = str(df1['SiteID'][i])
    else:
        SiteID = ''
    if str(df1['City'][i])!='nan':
        City  = str(df1['City'][i])
    else:
        City = ''
    if str(df1['County'][i])!='nan':
        County  = str(df1['County'][i])
    else:
        County = ''    
    if str(df1['Longtidute'][i])!='nan':
        Longtidute  = str(df1['Longtidute'][i])
    else:
        Longtidute = ''
    if str(df1['Latidute'][i])!='nan':
        Latidute  = str(df1['Latidute'][i])
    else:
        Latidute = ''
    if str(df1['Size_of_Area_m2'][i])!='nan':
        Size_of_Area_m2 = str(df1['Size_of_Area_m2'][i])
    else:
        Size_of_Area_m2 = ''
    if str(df1['IndustryType'][i])!='nan':
        IndustryType  = str(df1['IndustryType'][i])
    else:
        IndustryType = ''
   
    if str(df1['Enterprise_Size'][i])!='nan':
        Enterprise_Size  = str(df1['Enterprise_Size'][i])
    else:
        Enterprise_Size = ''
   
    if str(df1['Yr_of_started'][i])!='nan':
        Yr_of_started  = str(df1['Yr_of_started'][i])
    else:
        Yr_of_started  = ''

    if str(df1['Yr_of_Closed'][i])!='nan':
        Yr_of_Closed= str(df1['Yr_of_Closed'][i])
    else:
        Yr_of_Closed = ''

    if str(df1['Located_in_Industried_Park'][i])!='nan':
        Located_in_Industried_Park = str(df1['Located_in_Industried_Park'][i])
    else:
        Located_in_Industried_Park= ''

    if str(df1['LAND_USE'][i])!='nan':
        LAND_USE = str(df1['LAND_USE'][i])
    else:
        LAND_USE = ''

    if str(df1['Size_of_Production_Area_m2'][i])!='nan':
        Size_of_Production_Area_m2 = str(df1['Size_of_Production_Area_m2'][i])
    else:
        Size_of_Production_Area_m2= ''

    if str(df1['Size_of_Storage_Area_m2'][i])!='nan':
        Size_of_Storage_Area_m2 = str(df1['Size_of_Storage_Area_m2'][i])
    else:
        Size_of_Storage_Area_m2= ''

    if str(df1['Size_of_wasted_water_treatement_area_m2'][i])!='nan':
        Size_of_wasted_water_treatement_area_m2= str(df1['Size_of_wasted_water_treatement_area_m2'][i])
    else:
        Size_of_wasted_water_treatement_area_m2= ''

    if str(df1['Size_of_Treated_area_for_solid_wastes_m2'][i])!='nan':
        Size_of_Treated_area_for_solid_wastes_m2 = str(df1['Size_of_Treated_area_for_solid_wastes_m2'][i])
    else:
        Size_of_Treated_area_for_solid_wastes_m2 = ''

    if str(df1['Size_of_Key_Area_m2'][i])!='nan':
        Size_of_Key_Area_m2 = str(df1['Size_of_Key_Area_m2'][i])
    else:
        SSize_of_Key_Area_m2 = ''

    if str(df1['Unharden_Surface_on_Key_Area'][i])!='nan':
        Unharden_Surface_on_Key_Area = str(df1['Unharden_Surface_on_Key_Area'][i])
    else:
        Unharden_Surface_on_Key_Area= ''

    if str(df1['crack_on_the_hardened_ground_in_Key_area'][i])!='nan':
        crack_on_the_hardened_ground_in_Key_area = str(df1['crack_on_the_hardened_ground_in_Key_area'][i])
    else:
        crack_on_the_hardened_ground_in_Key_area= ''

    if str(df1['within_drainage_ditchesORseepage_pitsORreservoir'][i])!='nan':
        within_drainage_ditchesORseepage_pitsORreservoir= str(df1['within_drainage_ditchesORseepage_pitsORreservoir'][i])
    else:
       within_drainage_ditchesORseepage_pitsORreservoir= ''

    if str(df1['Within_Underground_storage_tanks_or_pipelines_for_products'][i])!='nan':
        Within_Underground_storage_tanks_or_pipelines_for_products = str(df1['Within_Underground_storage_tanks_or_pipelines_for_products'][i])
    else:
       Within_Underground_storage_tanks_or_pipelines_for_products = ''

    if str(df1['within_underground_pipelines_or_storage_tanks_for_wastewater'][i])!='nan':
        within_underground_pipelines_or_storage_tanks_for_wastewater= str(df1['within_underground_pipelines_or_storage_tanks_for_wastewater'][i])
    else:
        within_underground_pipelines_or_storage_tanks_for_wastewater = ''

    if str(df1['withing_Antiseeping_for_Underground_storage_tankORpipelineORreservoir'][i])!='nan':
        withing_Antiseeping_for_Underground_storage_tankORpipelineORreservoir= str(df1['withing_Antiseeping_for_Underground_storage_tankORpipelineORreservoir'][i])
    else:
       withing_Antiseeping_for_Underground_storage_tankORpipelineORreservoir= ''

    if str(df1['environmental_pollution_accident'][i])!='nan':
        environmental_pollution_accident = str(df1['environmental_pollution_accident'][i])
    else:
        environmental_pollution_accident= ''

    if str(df1['Nums_of_Polluted_Accidents'][i])!='nan':
        Nums_of_Polluted_Accidents= str(df1['Nums_of_Polluted_Accidents'][i])
    else:
       Nums_of_Polluted_Accidents= ''

    if str(df1['within_pollution_traces__on_topsoil'][i])!='nan':
        within_pollution_traces__on_topsoil = str(df1['within_pollution_traces__on_topsoil'][i])
    else:
       within_pollution_traces__on_topsoil = ''

    if str(df1['With_Bare_soil'][i])!='nan':
        With_Bare_soil= str(df1['With_Bare_soil'][i])
    else:
        With_Bare_soil = ''

    if str(df1['pollution_accidents_in_neighboring_plots'][i])!='nan':
        pollution_accidents_in_neighboring_plots= str(df1['pollution_accidents_in_neighboring_plots'][i])
    else:
       pollution_accidents_in_neighboring_plots= ''

    if str(df1['Selfdisposed_Hazardous_waste'][i])!='nan':
        Selfdisposed_Hazardous_waste= str(df1['Selfdisposed_Hazardous_waste'][i])
    else:
        Selfdisposed_Hazardous_waste= ''

    if str(df1['Hazardous_waste_left'][i])!='nan':
        Hazardous_waste_left = str(df1['Hazardous_waste_left'][i])
    else:
        Hazardous_waste_left= ''

    if str(df1['Facilities_and_structures_have_been_demolished_or_seriously_damaged'][i])!='nan':
        Facilities_and_structures_have_been_demolished_or_seriously_damaged = str(df1['Facilities_and_structures_have_been_demolished_or_seriously_damaged'][i])
    else:
        Facilities_and_structures_have_been_demolished_or_seriously_damaged= ''

    if str(df1['Shown_pollution_through_interview'][i])!='nan':
        Shown_pollution_through_interview = str(df1['Shown_pollution_through_interview'][i])
    else:
        Shown_pollution_through_interview= ''

    if str(df1['Abnormal_color_or_smell_of_groundwater'][i])!='nan':
        Abnormal_color_or_smell_of_groundwater = str(df1['Abnormal_color_or_smell_of_groundwater'][i])
    else:
        Abnormal_color_or_smell_of_groundwater= ''

    if str(df1['detected_Oily_substances__in_groundwater'][i])!='nan':
        detected_Oily_substances__in_groundwater = str(df1['detected_Oily_substances__in_groundwater'][i])
    else:
        detected_Oily_substances__in_groundwater= ''

    if str(df1['abnormal_groundwater_quality_shown'][i])!='nan':
        abnormal_groundwater_quality_shown = str(df1['abnormal_groundwater_quality_shown'][i])
    else:
        abnormal_groundwater_quality_shown= ''

    if str(df1['within_Artificial_filling_layer'][i])!='nan':
        within_Artificial_filling_layer = str(df1['within_Artificial_filling_layer'][i])
    else:
        within_Artificial_filling_layer= ''

    if str(df1['Groundwater_depth_m'][i])!='nan':
        Groundwater_depth_m = str(df1['Groundwater_depth_m'][i])
    else:
        Groundwater_depth_m= ''

    if str(df1['Permeability_of_saturated_zone'][i])!='nan':
        Permeability_of_saturated_zone = str(df1['Permeability_of_saturated_zone'][i])
    else:
        Permeability_of_saturated_zone= ''

    if str(df1['karst_geomorphology'][i])!='nan':
        karst_geomorphology = str(df1['karst_geomorphology'][i])
    else:
        karst_geomorphology= ''

    if str(df1['Annual_precipitation_mm'][i])!='nan':
        Annual_precipitation_mm = str(df1['Annual_precipitation_mm'][i])
    else:
        Annual_precipitation_mm= ''

    if str(df1['Population_within_500m_around'][i])!='nan':
        Population_within_500m_around = str(df1['Population_within_500m_around'][i])
    else:
        Population_within_500m_around= ''


    if str(df1['Distance_to_kindergarten_m'][i])!='nan':
        Distance_to_kindergarten_m = str(df1['Distance_to_kindergarten_m'][i])
    else:
        Distance_to_kindergarten_m= ''


    if str(df1['Distance_to_school_m'][i])!='nan':
        Distance_to_school_m = str(df1['Distance_to_school_m'][i])
    else:
        Distance_to_school_m= ''


    if str(df1['Distance_to_residentials_m'][i])!='nan':
        Distance_to_residentials_m = str(df1['Distance_to_residentials_m'][i])
    else:
        Distance_to_residentials_m= ''


    if str(df1['Distance_to_hospital_m'][i])!='nan':
        Distance_to_hospital_m = str(df1['Distance_to_hospital_m'][i])
    else:
        Distance_to_hospital_m= ''


    if str(df1['Distance_to_surface_water_m'][i])!='nan':
        Distance_to_surface_water_m = str(df1['Distance_to_surface_water_m'][i])
    else:
        Distance_to_surface_water_m= ''


    if str(df1['Detection_of_soil_pollutants_exceeding_the_standard'][i])!='nan':
        Detection_of_soil_pollutants_exceeding_the_standard = str(df1['Detection_of_soil_pollutants_exceeding_the_standard'][i])
    else:
        Detection_of_soil_pollutants_exceeding_the_standard= ''


    if str(df1['Detection_of_groundwater_pollutants_exceeding_the_standard'][i])!='nan':
        Detection_of_groundwater_pollutants_exceeding_the_standard = str(df1['Detection_of_groundwater_pollutants_exceeding_the_standard'][i])
    else:
        Detection_of_groundwater_pollutants_exceeding_the_standard= ''


    if str(df1['detected_soil_pollution'][i])!='nan':
        detected_soil_pollution = str(df1['detected_soil_pollution'][i])
    else:
        detected_soil_pollution= ''

    if str(df1['detected_groudwater_pollution'][i])!='nan':
        Ddetected_groudwater_pollution = str(df1['detected_groudwater_pollution'][i])
    else:
        detected_groudwater_pollution= ''


    if str(df1['Environmental_risk_score'][i])!='nan':
        Environmental_risk_score = str(df1['Environmental_risk_score'][i])
    else:
        Environmental_risk_score= ''





      
   
    
    
    node = Node("Site", 
                 name = name, SiteID=SiteID, City=City, County=County, Longtidute=Longtidute, Latidute=Latidute,
                 Size_of_Area_m2=Size_of_Area_m2, IndustryType=IndustryType, Enterprise_Size=Enterprise_Size,
                 Yr_of_started=Yr_of_started, Yr_of_Closed=Yr_of_Closed, Located_in_Industried_Park=Located_in_Industried_Park,
                 LAND_USE=LAND_USE, Size_of_Production_Area_m2=Size_of_Production_Area_m2, 
                 Size_of_Storage_Area_m2=Size_of_Storage_Area_m2, Size_of_wasted_water_treatement_area_m2=Size_of_wasted_water_treatement_area_m2, 
                 Size_of_Treated_area_for_solid_wastes_m2=Size_of_Treated_area_for_solid_wastes_m2,  
                 Size_of_Key_Area_m2=Size_of_Key_Area_m2,
                 Unharden_Surface_on_Key_Area=Unharden_Surface_on_Key_Area,
                 crack_on_the_hardened_ground_in_Key_area=crack_on_the_hardened_ground_in_Key_area,
                 within_drainage_ditchesORseepage_pitsORreservoir=within_drainage_ditchesORseepage_pitsORreservoir,
                 Within_Underground_storage_tanks_or_pipelines_for_products=Within_Underground_storage_tanks_or_pipelines_for_products,
                 within_underground_pipelines_or_storage_tanks_for_wastewater=within_underground_pipelines_or_storage_tanks_for_wastewater,
                 withing_Antiseeping_for_Underground_storage_tankORpipelineORreservoir=withing_Antiseeping_for_Underground_storage_tankORpipelineORreservoir,
                 environmental_pollution_accident=environmental_pollution_accident,
                 Nums_of_Polluted_Accidents=Nums_of_Polluted_Accidents,
                 within_pollution_traces__on_topsoil=within_pollution_traces__on_topsoil,
                 With_Bare_soil=With_Bare_soil,
                 pollution_accidents_in_neighboring_plots=pollution_accidents_in_neighboring_plots,
                 Selfdisposed_Hazardous_waste=Selfdisposed_Hazardous_waste,
                 Hazardous_waste_left=Hazardous_waste_left,
                 Facilities_and_structures_have_been_demolished_or_seriously_damaged=Facilities_and_structures_have_been_demolished_or_seriously_damaged,
                 Shown_pollution_through_interview=Shown_pollution_through_interview,
                 Abnormal_color_or_smell_of_groundwater=Abnormal_color_or_smell_of_groundwater,
                 detected_Oily_substances__in_groundwater=detected_Oily_substances__in_groundwater,
                 abnormal_groundwater_quality_shown=abnormal_groundwater_quality_shown,
                 within_Artificial_filling_layer=within_Artificial_filling_layer,
                 Groundwater_depth_m=Groundwater_depth_m,
                 Permeability_of_saturated_zone=Permeability_of_saturated_zone)
    
    graph.create(node)





"""
emitted Pollutants
"""
print('importing emitted pollutant......')
save_all_pollutant = []
column_pollutant = list(df2.columns)
col_p = column_pollutant[2:17]
for i in range(len(df2)): # len(df2)
    print(i)
    for j in range(0,len(col_p)-1,2):
        if str(df2[col_p[j]][i])!='nan':
            name = str(df2[col_p[j]][i]).strip()+'['+str(df2[col_p[j+1]][i])+']'
            if name not in save_all_pollutant:
                save_all_pollutant.append(name)
                node = Node("EmittedPollutant", emitted_pollutant = name)
                graph.create(node)
                
                block_node = matcher.match("site", name=df2['SiteName'][i]).first()
                relationship = Relationship(block_node, 'Emitted_Pollutant', node)
                graph.create(relationship)
            else:
                fei_node = matcher.match("EmittedPollutant", emitted_pollutant = name).first()
                block_node = matcher.match("block", name=df2['SiteName'][i]).first()
                relationship = Relationship(block_node, 'Emitted_Pollutant', fei_node)
                graph.create(relationship)
    name = 'Max_measured concentration_of_soil_pollutants：'+str(df2[col_p[-1]][i]).strip()
    if name not in save_all_pollutant:
        save_all_pollutant.append(name)
        node = Node("EmittedPollutant", emitted_pollutant = name)
        graph.create(node)
        
        block_node = matcher.match("block", name=df2['SiteName'][i]).first()
        relationship = Relationship(block_node, 'Emitted_Pollutant', node)
        graph.create(relationship)
    else:
       fei_node = matcher.match("EmittedPollutant", emitted_pollutant = name).first()
       block_node = matcher.match("block", name=df2['SiteName'][i]).first()
       relationship = Relationship(block_node, 'Emitted_Pollutant', fei_node)
       graph.create(relationship)
#save_all_危险化学品 = []
#column_危险化学品 = list(dff.columns)
#for i in range(100): # len(df_危险化学品)
#    print(i)
#    for j in column_危险化学品[2:85]:
#        if str(df_危险化学品[j][i])!='nan':
#            name = str(df_危险化学品[j][i]).strip()
#            if name not in save_all_危险化学品:
#                save_all_危险化学品.append(name)
#                node = Node("危险化学品", 危险化学品 = name)
#                graph.create(node)
#                
#                block_node = matcher.match("block", name=dff['地块名称'][i]).first()
#                relationship = Relationship(block_node, '危险化学品', node)
#                graph.create(relationship)
#            else:
#                fei_node = matcher.match("危险化学品", name=name).first()
#                block_node = matcher.match("block", name=dff['地块名称'][i]).first()
#                relationship = Relationship(block_node, '危险化学品', node)
#                graph.create(relationship)
                
"""
site_pollutant
"""
save_all_sitepollutant = []
column_sitepollutant = list(df3.columns)
print('Importing characteristic pollutants......')
for i in range(len(df3)): # len(df3)
    print(i)
    for j in column_sitepollutant[2:99]:
        if str(df3[j][i])!='nan':
            name = str(df3[j][i]).strip()
            if name not in save_all_sitepollutant:
                save_all_sitepollutant.append(name)
                node = Node("CharacteristicPollutant", CharacteristicPollutant = name)
                graph.create(node)
                
                block_node = matcher.match("block", name=df2['SiteName'][i]).first()
                relationship = Relationship(block_node, 'CharacteristicPollutant', node)
                graph.create(relationship)
            else:
                fei_node = matcher.match("CharacteristicPollutant", CharacteristicPollutant = name).first()
                block_node = matcher.match("block", name=df2['SiteName'][i]).first()
                relationship = Relationship(block_node, 'CharacteristicPollutant', fei_node)
                graph.create(relationship)




df1.dtypes
#aa = list(df1.columns)
#import codecs
#with codecs.open('列名.txt', 'w', encoding='utf_8') as fww:
#    for i in aa:
#        fww.write(i+'\n')
#    