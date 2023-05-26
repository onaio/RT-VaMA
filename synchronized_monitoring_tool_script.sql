 ---Form name: Synchronized Vaccination Monitoring Tool
 ---Objective: Monitors the status of the health facility
----Inform link: https://inform.unicef.org/uniceftemplates/635/847
-- To be able to execute the queries below, you'll need access to the following tables 
                  -- a. synchronized_vaccination_monitoring_tool - this is the table that has all the responses from the Synchronized Vaccination Monitoring Tool form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
                  -- g. synchronized_vaccination_monitoring_tool_vaccination_team_info - contains the vaccination team repeat group information 
--Note: The administrative hierarchy is dependent on the country that is implementing the RT-VaMA tool kit. On queries below, we used Philippines administrative hierarchy as our use case.

 ---This view creates a tidy table of the Synchronized Vaccination Monitoring Tool form
 ---For the query to execute successfully, the following tables are required:
                  -- a. synchronized_vaccination_monitoring_tool - this is the table that has all the responses from the Synchronized Vaccination Monitoring Tool form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
                  -- g. synchronized_vaccination_monitoring_tool_vaccination_team_info - contains the vaccination team repeat group information
                  -- h. synchronized_monitoring_tool csv file - contains the groups and the no. of questions within each group. This file is useful when calculating the proportion of each group based on the yes responses provided on the questions within the group
--- The following section(s) need to updated during customization:
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. The unnest sections can be updated if the list of questions and groups in the XLSForm is longer than the one provided
                  --- c. synchronized_monitoring_tool csv file can be updated if the no. of questions and list of groups in the XLSForm are more than the ones within the file

 -----The sub query below creates a view that only has the microplan and vaccine management questions
 create or replace view staging.microplan_vaccine_management as 
(
select 
  svmt.id,
  unnest(array[part1_indicator1,part1_indicator2,part1_indicator3,part1_indicator4,part1_indicator5,part1_indicator6,part1_indicator7,part1_indicator8,part1_indicator9,
  part1_indicator10,part1_indicator11,part1_indicator12,part1_indicator13,part1_indicator14,part1_indicator15,part1_indicator16,part1_indicator17,part1_indicator18,
  part1_indicator19,part1_indicator20,part1_indicator21,part1_indicator22,part1_indicator23,part1_indicator24,part2_indicator1,part2_indicator2,part2_indicator3,
  part2_indicator4,part2_indicator5,part2_indicator6,part2_indicator7,part2_indicator8,part2_indicator9,part2_indicator10,part2_indicator11,part2_indicator12,
  part2_indicator13,part2_indicator14,part2_indicator15,part2_indicator16,part2_indicator17,part2_indicator18,part2_indicator19,part2_indicator20,part2_indicator21,
  part2_indicator22,part2_indicator23,part2_indicator24,part2_indicator25,part2_indicator26,part2_indicator27,part2_indicator28,part2_indicator29,part2_indicator30,
  part2_indicator28a::text,part2_indicator29a::text,part2_indicator30a::text]) as indicators_value,
  unnest(array[part1_indicator1_remarks,part1_indicator2_remarks,part1_indicator3_remarks,part1_indicator4_remarks,part1_indicator5_remarks,part1_indicator6_remarks,
  part1_indicator7_remarks,part1_indicator8_remarks,part1_indicator9_remarks,part1_indicator10_remarks,part1_indicator11_remarks,part1_indicator12_remarks,part1_indicator13_remarks,
  part1_indicator14_remarks,part1_indicator15_remarks,part1_indicator16_remarks,part1_indicator17_remarks,part1_indicator18_remarks,part1_indicator19_remarks,part1_indicator20_remarks,
  part1_indicator21_remarks,part1_indicator22_remarks,part1_indicator23_remarks,part1_indicator24_remarks,part2_indicator1_remarks,part2_indicator2_remarks,part2_indicator3_remarks,
  part2_indicator4_remarks,part2_indicator5_remarks,part2_indicator6_remarks,part2_indicator7_remarks,part2_indicator8_remarks,part2_indicator9_remarks,part2_indicator10_remarks,
  part2_indicator11_remarks,part2_indicator12_remarks,part2_indicator13_remarks,part2_indicator14_remarks,part2_indicator15_remarks,part2_indicator16_remarks,part2_indicator17_remarks,
  part2_indicator18_remarks,part2_indicator19_remarks,part2_indicator20_remarks,part2_indicator21_remarks,part2_indicator22_remarks,part2_indicator23_remarks,part2_indicator24_remarks,
  part2_indicator25_remarks,part2_indicator26_remarks,part2_indicator27_remarks,part2_indicator28_remarks,part2_indicator29_remarks,part2_indicator30_remarks,part2_indicator28b,
  part2_indicator29b,part2_indicator30b]) as indicators_remarks,
  unnest(array['Presence of data board','Presence of health center microplan','Presence of spot map','Indication of population/specific target',
  'Inclusion of activities for social preparation','Inclusion of dialogues with local officials/CSG','Public announcements are made','Evidence that social mobilization were done',
  'Presence of activities to enable access in hard to reach areas are expected','Training of vaccination teams on comms and social mobilization','Presence of daily itinerary schedule',
  'Presence of specific vaccination strategy','Supervisory plan','Presence of separate sheet for vaccines and other logistic calculations','Enough campaign forms',
  'Enough mother/child book or vaccination cards','Presence of transportation support','Response/referral for AEFI','Presence of contingency plan to include emergencies in case of absence of vaccination team member',
  'Schedule for mop ups','Plan for RCA intra-campaign','Evidence of regular feedback meeting','Health care waste plan','Follow up visits','Presence of health facility management plan',
  'Presence of continuous electricity supply','Presence of generator/solar power that can be used in case of intermittent power supply','Presence of refrigiration that can be used for vaccine',
  'Vaccines placed in separate box','Proper label is used for vaccine','Vaccines are stored with appropriate temperature','Presence of adequate temperature monitoring devices',
  'Conduct of regular temperature monitoring','Proper temperature monitoring','Note of temperature breach','Availability of ice pack freezing capacity','Recording of vaccines that are issued daily',
  'Proper filling up of forms','Presence of enough vaccine carriers','Presence of enough ice packs','Providing immunzation at a fixed post','Presence of vaccine carrier that is separately label',
  'Use of resealable plastic','Use of resealable plastic for used vials','Return of reusable vials','Accounting of all collected vials','Presence of vaccine accountability monitor',
  'Placing of collected vials in a secured container','Empty vials, sealed properly','Returning of un-opened/un used vial','Account of used and unused vials','Missing vials identified',
  'Replaced vials identified','Damaged vials','Number of missing vials', 'Number of replaced vials','Number of damaged vials']) as indicators_label,
  unnest(array['Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check',
  'Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check',
  'Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check',
  'Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check',
  'Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check',
  'Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check',
  'Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management',
  'Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management',
  'Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management',
  'Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management',
  'Vaccine management'])as indicators_category
from templates.synchronized_vaccination_monitoring_tool svmt
);


----The sub query below creates a view that has the vaccination site questions
create or replace view staging.vaccination_site_questions_monitoring_tool as 
(
select 
  svmtvti.parent_id,
  unnest(array[part3_indicator1,part3_indicator2,part3_indicator3,part3_indicator4,part3_indicator5,part3_indicator6,part3_indicator7,part3_indicator8,part3_indicator9,
  part3_indicator10,part3_indicator11,part3_indicator12,part3_indicator13,part3_indicator14,part3_indicator15,part3_indicator16,part3_indicator17,part3_indicator18])as indicators_value,
  unnest(array[part3_indicator1_remarks,part3_indicator2_remarks,part3_indicator3_remarks,part3_indicator4_remarks,part3_indicator5_remarks,part3_indicator6_remarks,
  part3_indicator7_remarks,part3_indicator8_remarks,part3_indicator9_remarks,part3_indicator10_remarks,part3_indicator11_remarks,part3_indicator12_remarks,part3_indicator13_remarks,
  part3_indicator14_remarks,part3_indicator15_remarks,part3_indicator16_remarks,part3_indicator17_remarks,part3_indicator18_remarks])as indicators_remarks,
  unnest(array['Supervisor visits and supervises team','Microplan followed and reviewed','Vaccination strategies followed','Vaccine carrier carried','Use of resealable plastic',
  'Vaccines are stored with appropriate temperatures','Encountered difficulties at site','Vaccines properly recorded','Finger markings correctly applied',
  'Doses and vaccinated children properly recorded','Doors properly marked','Attaining of daily targets tracked','Healthcare waste appropriately handled',
  'Vaccination team wearing PPE','Hand hygiene practised','Caregivers reminded to follow up routine vaccination','Caregivers reminded what to do in case of reactions',
  'Members asking questions related to AFP'])as indicators_label,
  unnest(array['At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site',
  'At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site',
  'At Vaccination site','At Vaccination site','At Vaccination site'])as indicators_category
from templates.synchronized_vaccination_monitoring_tool_vaccination_team_info svmtvti  
);



create or replace view staging.monitoring_tool as
(
with monitoring_tool as 
(
select id, indicators_value, indicators_remarks, indicators_label, indicators_category from staging.microplan_vaccine_management
union all
select parent_id,indicators_value, indicators_remarks, indicators_label, indicators_category from staging.vaccination_site_questions_monitoring_tool
)
select 
  mt.id,
  svmt.date_vaccination_activity,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  svmt.admin4_lat::real as latitude,
  svmt.admin4_long::real as longitude,
  a5.label as admin5,
  svmt.vaccine_label, mt.indicators_value,
  mt.indicators_remarks, 
  mt.indicators_label,
  mt.indicators_category,
  smt.no_of_questions  
from monitoring_tool mt
left join csv.synchronized_monitoring_tool smt on mt.indicators_category=smt.indicators_category ---adds no.of questions in the category
left join templates.synchronized_vaccination_monitoring_tool svmt on mt.id=svmt.id ---adds the missing fields in the repeat group
left join csv.admin1 a1 on svmt.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on svmt.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on svmt.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on svmt.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on svmt.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
where a5.label is not null
);


-----Synchronized Vaccination Monitoring Tool
---This view assigns proportions to reporting facilities based on the category performance
---For the query to execute successfully, the following tables are required:
                  -- a. synchronized_vaccination_monitoring_tool - this is the table that has all the responses from the Synchronized Vaccination Monitoring Tool form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
                  -- g. synchronized_vaccination_monitoring_tool_vaccination_team_info - contains the vaccination team repeat group information
                  -- h. synchronized_monitoring_tool csv file - contains the groups and the no. of questions within each group. This file is useful when calculating the proportion of each group based on the yes responses provided on the questions within the group

create or replace view staging.monitored_facilities_overall_proportion as 
(
with monitoring_tool as 
(
select * from staging.monitoring_tool mt
),
yes_responded_questions as
(
select 
  mt.id,
  mt.date_vaccination_activity,
  mt.admin1,
  mt.admin2,
  mt.admin3,
  mt.admin4,
  mt.admin5,
  mt.latitude,
  mt.longitude,
  mt.vaccine_label as vaccine_administered,
  mt.indicators_category,
  mt.no_of_questions,
  count(mt.id) filter (where mt.indicators_value='Yes') as count_yes_questions,
  (((count(mt.id) filter (where mt.indicators_value='Yes'))::float /(no_of_questions)::float)) as yes_category_proportion 
from monitoring_tool mt
where mt.indicators_value is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12
)
select 
yrq.id,
yrq.date_vaccination_activity,
yrq.admin1,
yrq.admin2,
yrq.admin3,
yrq.admin4,
yrq.admin5,
yrq.latitude,
yrq.longitude,
yrq.vaccine_administered,
(sum(case when yrq.yes_category_proportion=1 then 1 else 0 end)::float/ 3)*100 as overall_facility_proportion,
case 
	when ((sum(case when yrq.yes_category_proportion=1 then 1 else 0 end)::float/ 3)*100) >=0 and ((sum(case when yrq.yes_category_proportion=1 then 1 else 0 end)::float/ 3)*100) < 50 then '<50%'
	when ((sum(case when yrq.yes_category_proportion=1 then 1 else 0 end)::float/ 3)*100) >=50 and ((sum(case when yrq.yes_category_proportion=1 then 1 else 0 end)::float/ 3)*100) <= 99 then '50-99%'
	when ((sum(case when yrq.yes_category_proportion=1 then 1 else 0 end)::float/ 3)*100) =100 then '100%'
end as overall_proportion_category ---assigns the category labels 
from yes_responded_questions yrq
group by 1,2,3,4,5,6,7,8,9,10
);

----This query creates a view of the vaccinated children in the monitoring tool
---For the query to execute successfully, the following tables are required:
                  -- a. synchronized_vaccination_monitoring_tool - this is the table that has all the responses from the Synchronized Vaccination Monitoring Tool form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
                  -- g. province_iso2_codes - contains the iso codes for admin 2. The iso codes are useful when working on country maps on Superset because they allow mapping of the form data onto the map provided by Superset
--- The following section(s) need to updated during customization:
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. province_iso2_codes needs to be updated to match the admin 2 iso codes of the reporting country office
create or replace view staging.vaccinated_children_under_monitoring_tool as
(
select  
svmt.id,
a1.label as admin1,
a2.label as admin2,
a3.label as admin3,
a4.label as admin4,
svmt.admin4_lat::real as latitude,
svmt.admin4_long::real as longitude,
a5.label as admin5,
svmt.date_vaccination_activity,
svmt.vaccine_label as vaccine_administered,
svmt.total_vaccinated_as_time_visit,
svmt.hc_target,
pic.iso2_code,
svmt.submitted_at,
svmt.modified_at,
svmt.enumerator
from templates.synchronized_vaccination_monitoring_tool svmt 
left join csv.admin1 a1 on svmt.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on svmt.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on svmt.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on svmt.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on svmt.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join csv.province_iso2_codes pic on pic.admin2_id::text=a2.name::text ---
);



------POWER BI VIEWS
---The PowerBI connector currently pulls data from views within the public schema. Hence the repointing to the public schema
create or replace view public.monitoring_tool as 
(
select * from staging.monitoring_tool mt 
);


----Monitored facilities overall proportion
create or replace view public.monitored_facilities_overall_proportion as 
(
select * from staging.monitored_facilities_overall_proportion
);


-----Vaccinated children under the monitoing tool
create or replace view public.vaccinated_children_under_monitoring_tool as 
(
select 
    id,
    admin1,
    admin2,
    admin3,
    admin4,
    latitude,
    longitude,
    admin5,
    date_vaccination_activity,
    vaccine_administered,
    total_vaccinated_as_time_visit,
    hc_target,
    iso2_code,
    submitted_at::date,
    modified_at::date,
    enumerator
from staging.vaccinated_children_under_monitoring_tool vcumt 
);

