----Form name: Health Center Level Monitoring and Assessment of Readiness
--- Objective: This form is used to assess whether the facilities are ready for the vaccination campaign. It is usually administered between 1week and 6 months before the campaign starts
----Inform link: https://inform.unicef.org/uniceftemplates/635/761
-- Within this script there are 2 SQL queries, one that unnests the forms questions and another one that calculates the readiness proportion based on the question categories that are available.
-- To be able to execute the queries below, you'll need access to the following tables 
                  -- a. health_center_level_monitoring_and_assessment_of_readiness - this is the table that has all the responses from the Health Center Level Monitoring and Assessment of Readiness form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
--Note: The administrative hierarchy is dependent on the country that is implementing the RT-VaMA tool kit. On queries below, we used Philippines administrative hierarchy as our use case.

--- The connectors normally create a seperate labels table that can be joined to the other tables using SQL
-----This query creates the Health Center Level Monitoring and Assessment of Readiness labels table

create or replace view staging.hcl_monitoring_assessment_labels as 
(
-- Extract the json column we want, limit by 1 row since the data has a row for each filled record.
with dd as
(
select 
json -> 'xform:choices' as data 
from templates.registry
where uri = 'health_center_level_monitoring_and_assessment_of_readiness?t=json&v=202301201348'
limit 1
), 
-- Strip down the json to the columns we want, question and label details (this includes the language(s) in the form).
unnest_1 as (
select 
js.key as question, 
js.value as label_details
from dd, jsonb_each(dd.data) as js
), 
-- Strip down the json further this time getting the code (name column in the xlsform)
unnest_2 as (
select unnest_1.question, 
js1.key as code, js1.value as language
from unnest_1, jsonb_each(unnest_1.label_details) as js1
)
-- Since the data has a slash(/) sperator for the choices question, we are reversing the order then get the first object before the first slash then reverse it back.
select 
    reverse(split_part(reverse(unnest_2.question), '/', 1)) question,
    unnest_2.code,
    -- Since languages are dynamic,we add this manually for each of the languages used ("und" is when no language has been specified on the form)
    unnest_2.language ->> 'und' AS label
from unnest_2
);


---This view creates a tidy table of the Health Center Level Monitoring and Assessment of Readiness form
--- This query was developed based on the indicators that were to be developed. 
-- The unnest sections of the query enables one to be able to have a tidy table that has relevant columns that can be used on different types of visualizations
---For the query to execute successfully, the following tables are required:
                  -- a. health_center_level_monitoring_and_assessment_of_readiness - Health Center Level Monitoring and Assessment of Readiness form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
                  -- g. hc_assessment_questions_per_category - contains the no. of questions for each category
--- The following sections need to updated during customization:
           --a. Indicators_label unnest section: the set on questions need to be updated based on how they have been labelled within the customized Health Center Level Monitoring and Assessment of Readiness form
           --b. The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country reporting_office adopting the tool
           --c. If the country office survey has extra categories then the no. of categories that has been used to calculate the proportion needs to be adjusted from 8 to the co no.of categories.
--NB: The question categories were derived from the groups available within the XLSForm

----The sub query below unnests the form questions
create or replace view staging.hc_assessment as 
----Unnests the questions and assigns label and categories to the respective question values. This section allows one to have a simplified long table of all the set of questions and responses within the Health Center Level Monitoring and Assessment of Readiness form
(
select 
hcl.id,
unnest(array[microplan_indicator1,microplan_indicator2,microplan_indicator3,microplan_indicator4,microplan_indicator5,microplan_indicator6,microplan_indicator7,microplan_indicator8,
microplan_indicator9,logistics_indicator1,logistics_indicator2,logistics_indicator3,logistics_indicator4,logistics_indicator5,logistics_indicator6,logistics_indicator7,
social_mob_indicator1,social_mob_indicator2,social_mob_indicator3,imm_safety_indicator1,imm_safety_indicator2,supervision_indicator1,supervision_indicator2,supervision_indicator3,
supervision_indicator4,supervision_indicator5,reporting_indicator1,reporting_indicator2,reporting_indicator3,vacc_mngt_indicator1,vacc_mngt_indicator2,vacc_mngt_indicator3,
vacc_mngt_indicator4,hr_indicator1]) as indicators_value, ----creates a column that has the yes/no responses of the respective questions
unnest(array[microplan_indicator1_remarks,microplan_indicator2_remarks,microplan_indicator3_remarks,microplan_indicator4_remarks,microplan_indicator5_remarks,
microplan_indicator6_remarks,microplan_indicator7_remarks,microplan_indicator8_remarks,microplan_indicator9_remarks,logistics_indicator1_remarks,logistics_indicator2_remarks,
logistics_indicator3_remarks,logistics_indicator4_remarks,logistics_indicator5_remarks,logistics_indicator6_remarks,logistics_indicator7_remarks,social_mob_indicator1_remarks,
social_mob_indicator2_remarks,social_mob_indicator3_remarks,imm_safety_indicator1_remarks,imm_safety_indicator2_remarks,supervision_indicator1_remarks,supervision_indicator2_remarks,
supervision_indicator3_remarks,supervision_indicator4_remarks,supervision_indicator5_remarks,reporting_indicator1_remarks,reporting_indicator2_remarks,reporting_indicator3_remarks,
vacc_mngt_indicator1_remarks,vacc_mngt_indicator2_remarks,vacc_mngt_indicator3_remarks,vacc_mngt_indicator4_remarks,hr_indicator2_remarks]) as indicators_remarks, ----creates a column that has the remarks responses of the respective questions
unnest(array['Inclusion of all areas in the health center microplan','List of all transit and congregation points,markets and religious gathering available',
'A plan to reach high-risk populations included','Daily activity plans for the teams are available','Special strategies clearly planned','Maps show catchment areas',
'Logistics and other resource estimations are complete','List of local influencers and contact details available','Vaccine and waste management plan in place',
'Cold chain capacity and contigency plans for vaccine storage available','Availability of adequate quantity of vaccines','Availability of adequate quantity of vaccination essentials(eg. vaccine carries,ice packs)',' 
Face mask and hand hygiene available','Other logistics received (finger markers etc)','Logistics transport available to supply all areas','Contigency plan in place for replenishment when stocks run low',
'Engagement of leaders/officials for campaign announcements and meetings confirmed','Display of promotion materials in conspicuous places',
'Community aware about the assigned date and venue of vaccination sessions','Supervisors know how to report AEFI and communicate risk in case of AEFIs',
'AEFI investigation forms and SOPs available with supervisors','Monitoring and supervision plan available','Supervisors trained for conducting team monitoring and RCMs',
'Required checklists and templates available','Mop-up system in place in areas with un-immunized/missed children after RCA','Daily monitoring of coverage data and feedback system available',
'Daily collection and consolidation of tally sheets system available','Mechanism in place for submission of reports','ODK orientation conducted','Logistics focal point assigned and trained on vaccine management',
'Separate space allocation for vaccine and clear labelling in refrigerator','Required recording and reporting templates available','System in place for vaccine recall,accuntability,collection,handover and reporting',
'Adequate number of vaccinators and recorders']) as indicators_label,----creates a column that has the labels of the respective questions
unnest(array['Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Logistics Supply',
'Logistics Supply','Logistics Supply','Logistics Supply','Logistics Supply','Logistics Supply','Logistics Supply','Social Mobilization','Social Mobilization','Social Mobilization',
'Immunization Safety','Immunization Safety','Supervision and Monitoring','Supervision and Monitoring','Supervision and Monitoring','Supervision and Monitoring','Supervision and Monitoring',
'Reporting System','Reporting System','Reporting System','Vaccine Management','Vaccine Management','Vaccine Management','Vaccine Management','Human Resource']) as indicators_category ----creates a column that has the category of the respective questions
from templates.health_center_level_monitoring_and_assessment_of_readiness hcl ---the raw table from Inform
);


create or replace view staging.hcl_monitoring_assessment as
with latest_assessment_date as ---Retrieves the latest facility assessment date 
(
select 
     admin5, max(hcl.assessment_date) as latest_assessment
from templates.health_center_level_monitoring_and_assessment_of_readiness hcl 
group by 1 
),
latest_assessment_id as ----Retrieves the record id of the latest facility assessment
(
select  la.admin5, hc.id, la.latest_assessment
from  latest_assessment_date  la
inner join templates.health_center_level_monitoring_and_assessment_of_readiness  hc
 on la.admin5 = hc.admin5 and la.latest_assessment = hc.assessment_date 
),
vaccine_administered as ---unnests the select multiple vaccine administered question
(
select 
   id,
   trim(unnest(string_to_array(REGEXP_REPLACE(hcl.vaccine_administered::text,'[\[\]"]', '', 'g'),','))) as vaccine_administered 
from templates.health_center_level_monitoring_and_assessment_of_readiness hcl
)
select
ha.id,
la.latest_assessment as assessment_date,
null as vaccine_label,
a1.label as admin1,
a2.label as admin2,
a3.label as admin3,
a4.label  as admin4,
hcl.admin4_lat::real as latitude,
hcl.admin4_long:: real as longitude,
a5.label as admin5,
ha.indicators_value,
ha.indicators_remarks,
trim(ha.indicators_label) as indicators_label,
ha.indicators_category,
haqpc.no_of_questions,
hcl.submitted_at,
hcl.modified_at,
hcl.enumerator 
from staging.hc_assessment ha
left join latest_assessment_id la on ha.id = la.id
---left join vaccine_administered va on ha.id=va.id
left join csv.hc_assessment_questions_per_category haqpc on ha.indicators_category=haqpc.indicators_category ----The hc_assessment_questions_per_category table has the list of categories with no.of questions per category
left join templates.health_center_level_monitoring_and_assessment_of_readiness hcl on ha.id=hcl.id 
left join csv.admin1 a1 on hcl.admin1=a1.name::text and ha.id=hcl.id---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on hcl.admin2=a2.name::text and ha.id=hcl.id ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on hcl.admin3=a3.name::text and ha.id=hcl.id ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on hcl.admin4=a4.name::text and ha.id=hcl.id ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on hcl.admin5=a5.name::text and ha.id=hcl.id ---Adds admin 5 labels using the admin name column
----left join staging.hcl_monitoring_assessment_labels hcll on va.vaccine_administered=hcll.code and hcll.question='vaccine_administered'



----This query creates a view that has the facilities proportions based on the responses provided to the questions during the assessment
---For the query to execute successfully, the following tables are required:
                  -- a. health_center_level_monitoring_and_assessment_of_readiness - Health Center Level Monitoring and Assessment of Readiness form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
                  -- g. hc_assessment_questions_per_category - contains the no. of questions for each category
create or replace view staging.ready_facilities as 
(
with hcl_questions as 
(
select * from staging.hcl_monitoring_assessment hma 
),
yes_responses as 
---Gets a count of yes responses per category and calculates the proportion of each category based on the no. of questions within the category and number of yes responses within the category
(
select
hcl.admin1,
hcl.admin2,
hcl.admin3,
hcl.admin4,
hcl.admin5,
hcl.latitude,
hcl.longitude,
hcl.indicators_category,
(count(id) filter (where indicators_value='Yes')) as yes_count,
hcl.no_of_questions,
((count(id) filter (where indicators_value='Yes'))::float/ hcl.no_of_questions::float) as yes_category_proportion
from hcl_questions hcl
group by 1,2,3,4,5,6,7,8,10
)
----Calculates the overall readiness score of the facility based on the no. of categories available and assigns readiness score labels to the calculated scores per facility
select 
yrp.admin1,
yrp.admin2,
yrp.admin3,
yrp.admin4,
yrp.admin5,
yrp.latitude,
yrp.longitude,
sum(yrp.yes_category_proportion)::numeric as readiness_value,
case when sum(yrp.yes_category_proportion)=8 then 'Yes' else 'No' end as facility_ready,
round(((SUM(
case when yrp.yes_category_proportion=1 then 1 else 0 end)::float / 8)*100)::int,0) as category_value,
case   
	when (round(((SUM(case when yrp.yes_category_proportion=1 then 1 else 0 end)::float / 8)*100)::int,0)) >=0 and (round(((SUM(case when yrp.yes_category_proportion=1 then 1 else 0 end)::float / 8)*100)::int,0))<50 then '<50%'
	when (round(((SUM(case when yrp.yes_category_proportion=1 then 1 else 0 end)::float / 8)*100)::int,0)) >=50 and (round(((SUM(case when yrp.yes_category_proportion=1 then 1 else 0 end)::float / 8)*100)::int,0))<=99 then '50-99%'
	when (round(((SUM(case when yrp.yes_category_proportion=1 then 1 else 0 end)::float / 8)*100)::int,0)) =100 then '100%' else null
end as category_label
from yes_responses yrp
group by 1,2,3,4,5,6,7
);


------POWER BI VIEWS
---The PowerBI connector currently pulls data from views within the public schema. Hence the repointing to the public schema
----Health Assessment Readiness
create or replace view public.hcl_monitoring_assessment as 
(
select 
     id,
     assessment_date,
     vaccine_label,
     admin1,
     admin2,
     admin3,
     admin4,
     latitude,
     longitude,
     admin5,
     indicators_value,
     indicators_remarks,
     indicators_label,
     indicators_category,
     no_of_questions,
     submitted_at::date,
     modified_at::date,
     enumerator
from staging.hcl_monitoring_assessment
);

----Ready facilities
create or replace view public.ready_facilities as 
(
select * from staging.ready_facilities
);

