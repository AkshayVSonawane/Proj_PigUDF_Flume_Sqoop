register /home/acadgild/Downloads/pig_xml.jar;

xml_input_data = load '/Akshay/proj/StatewiseDistrictwisePhysicalProgress.xml' using pig.XML.newloader('row') as (doc:chararray);


data_from_xml_ip = foreach xml_input_data GENERATE 
FLATTEN(REGEX_EXTRACT_ALL(doc,'<row>\\s*<State_Name>(.*)</State_Name>\\s*<District_Name>(.*)</District_Name>\\s*<Project_Objectives_IHHL_BPL>(.*)</Project_Objectives_IHHL_BPL>\\s*<Project_Objectives_IHHL_APL>(.*)</Project_Objectives_IHHL_APL>\\s*<Project_Objectives_IHHL_TOTAL>(.*)</Project_Objectives_IHHL_TOTAL>\\s*<Project_Objectives_SCW>(.*)</Project_Objectives_SCW>\\s*<Project_Objectives_School_Toilets>(.*)</Project_Objectives_School_Toilets>\\s*<Project_Objectives_Anganwadi_Toilets>(.*)</Project_Objectives_Anganwadi_Toilets>\\s*<Project_Objectives_RSM>(.*)</Project_Objectives_RSM>\\s*<Project_Objectives_PC>(.*)</Project_Objectives_PC>\\s*<Project_Performance-IHHL_BPL>(.*)</Project_Performance-IHHL_BPL>\\s*<Project_Performance-IHHL_APL>(.*)</Project_Performance-IHHL_APL>\\s*<Project_Performance-IHHL_TOTAL>(.*)</Project_Performance-IHHL_TOTAL>\\s*<Project_Performance-SCW>(.*)</Project_Performance-SCW>\\s*<Project_Performance-School_Toilets>(.*)</Project_Performance-School_Toilets>\\s*<Project_Performance-Anganwadi_Toilets>(.*)</Project_Performance-Anganwadi_Toilets>\\s*<Project_Performance-RSM>(.*)</Project_Performance-RSM>\\s*<Project_Performance-PC>(.*)</Project_Performance-PC>\\s*</row>'));


register computePercentage.jar;

district_Percentage = foreach data_from_xml_ip generate $1,  udf_ComputePercentage.ComputePercentage($0,$1,(int)$2,(int)$3,(int)$4,(int)$5,(int)$6,(int)$7,(int)$8,(int)$9,(int)$10,(int)$11,(int)$12,(int)$13,(int)$14,(int)$15,(int)$16,(int)$17);

district_80_Percentage = filter district_Percentage by $1>=80.00;

store district_80_Percentage into '/Akshay/proj/pig_result8/' using PigStorage(',');

