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

create or replace view airbyte_removed_group.hcl_monitoring_assessment_labels as 
(
select
    field as question,
    value as code,
    label as label
from airbyte.chc_health_center_le__sessment_of_readiness
);

---The query that removes the group names from Airbyte.
---When creating an Airbyte connector, the connector appends the group name to the field name.
create or replace view airbyte_removed_group.hcl_monitoring_and_assessment_of_readiness as
(
with data as
(
select
"microplan/microplan_indicator8"::varchar as microplan_indicator8,
"microplan/microplan_indicator1_remarks"::varchar as microplan_indicator1_remarks,
"_xform_id_string"::varchar as "_xform_id_string",
"microplan/microplan_indicator7"::varchar as microplan_indicator7,
"microplan/microplan_indicator9"::varchar as microplan_indicator9,
"microplan/microplan_indicator4"::varchar as microplan_indicator4,
"remarks_grp/end_note"::varchar as end_note,
"microplan/microplan_indicator3"::varchar as microplan_indicator3,
"assessment_details/enumerator"::varchar as enumerator,
"microplan/microplan_indicator6"::varchar as microplan_indicator6,
"_review_comment"::varchar as "_review_comment",
"microplan/microplan_indicator5"::varchar as microplan_indicator5,
"microplan/microplan_indicator2"::varchar as microplan_indicator2,
"microplan/microplan_indicator1"::varchar as microplan_indicator1,
"logistics_supply/log__cs_indicator7_remarks"::varchar as logistics_indicator7_remarks,
"_attachments"::varchar as "_attachments",
"social_mobilization/__ob_indicator1_remarks"::varchar as social_mob_indicator1_remarks,
"microplan/microplan_indicator5_remarks"::varchar as microplan_indicator5_remarks,
"_submission_time"::timestamp as submitted_at,
"logistics_supply/log__cs_indicator3_remarks"::varchar as logistics_indicator3_remarks,
"_version"::varchar as "_version",
"supervision/supervision_indicator3_remarks"::varchar as supervision_indicator3_remarks,
"vaccine_management/v__gt_indicator3_remarks"::varchar as vacc_mngt_indicator3_remarks,
"immunization_safety/__ty_indicator1_remarks"::varchar as imm_safety_indicator1_remarks,
"_date_modified"::timestamp as modified_at,
"assessment_details/assessment_time"::varchar as assessment_time,
"device_id"::varchar as "device_id",
"supervision/supervision_indicator2_remarks"::varchar as supervision_indicator2_remarks,
"_status"::varchar as "_status",
"_media_all_received"::varchar as "_media_all_received",
"_edited"::varchar as "_edited",
"social_mobilization/social_mob_indicator1"::varchar as social_mob_indicator1,
"microplan/microplan_indicator8_remarks"::varchar as microplan_indicator8_remarks,
"social_mobilization/social_mob_indicator2"::varchar as social_mob_indicator2,
"remarks_grp/remarks"::varchar as remarks,
"social_mobilization/social_mob_indicator3"::varchar as social_mob_indicator3,
"assessment_details/reporting_office"::varchar as reporting_office,
"human_resource/hr_indicator2_remarks"::varchar as hr_indicator2_remarks,
"logistics_supply/logistics_indicator1"::varchar as logistics_indicator1,
"logistics_supply/logistics_indicator2"::varchar as logistics_indicator2,
"logistics_supply/logistics_indicator5"::varchar as logistics_indicator5,
"logistics_supply/logistics_indicator6"::varchar as logistics_indicator6,
"microplan/microplan_indicator2_remarks"::varchar as microplan_indicator2_remarks,
"logistics_supply/logistics_indicator3"::varchar as logistics_indicator3,
"logistics_supply/logistics_indicator4"::varchar as logistics_indicator4,
"logistics_supply/logistics_indicator7"::varchar as logistics_indicator7,
"_total_media"::varchar as "_total_media",
"logistics_supply/log__cs_indicator6_remarks"::varchar as logistics_indicator6_remarks,
"vaccine_management/v__gt_indicator4_remarks"::varchar as vacc_mngt_indicator4_remarks,
"_duration"::varchar as "_duration",
"geographical_location/admin1"::varchar as admin1,
"geographical_location/admin0"::varchar as admin0,
"remarks_grp/endnote2"::varchar as endnote2,
"assessment_details/vaccine_administered"::varchar as vaccine_administered,
"_media_count"::varchar as "_media_count",
"_submitted_by"::varchar as "_submitted_by",
"assessment_details/assessment_date"::varchar as assessment_date,
"microplan/microplan_indicator9_remarks"::varchar as microplan_indicator9_remarks,
"reporting_system/rep__ng_indicator1_remarks"::varchar as reporting_indicator1_remarks,
"assessment_details/vaccine_label"::varchar as vaccine_label,
"_tags"::varchar as "_tags",
"_review_status"::varchar as "_review_status",
"immunization_safety/imm_safety_indicator1"::varchar as imm_safety_indicator1,
"immunization_safety/imm_safety_indicator2"::varchar as imm_safety_indicator2,
"human_resource/hr_indicator2"::varchar as hr_indicator2,
"logistics_supply/log__cs_indicator5_remarks"::varchar as logistics_indicator5_remarks,
"human_resource/hr_indicator1"::varchar as hr_indicator1,
"vaccine_management/v__gt_indicator1_remarks"::varchar as vacc_mngt_indicator1_remarks,
"intro"::varchar as "intro",
"geographical_location/admin5"::varchar as admin5,
"supervision/supervision_indicator5"::varchar as supervision_indicator5,
"geographical_location/admin4"::varchar as admin4,
"geographical_location/admin3"::varchar as admin3,
"supervision/supervision_indicator3"::varchar as supervision_indicator3,
"geographical_location/admin2"::varchar as admin2,
"supervision/supervision_indicator4"::varchar as supervision_indicator4,
"_notes"::varchar as "_notes",
"supervision/supervision_indicator1"::varchar as supervision_indicator1,
"supervision/supervision_indicator2"::varchar as supervision_indicator2,
"microplan/microplan_indicator3_remarks"::varchar as microplan_indicator3_remarks,
"_geolocation"::varchar as "_geolocation",
"supervision/supervision_indicator5_remarks"::varchar as supervision_indicator5_remarks,
"geographical_location/admin4_long"::varchar as admin4_long,
"logistics_supply/log__cs_indicator2_remarks"::varchar as logistics_indicator2_remarks,
"microplan/microplan_indicator6_remarks"::varchar as microplan_indicator6_remarks,
"social_mobilization/__ob_indicator3_remarks"::varchar as social_mob_indicator3_remarks,
"_bamboo_dataset_id"::varchar as "_bamboo_dataset_id",
"vaccine_management/vacc_mngt_indicator4"::varchar as vacc_mngt_indicator4,
"reporting_system/rep__ng_indicator2_remarks"::varchar as reporting_indicator2_remarks,
"_id"::varchar as id,
"vaccine_management/vacc_mngt_indicator2"::varchar as vacc_mngt_indicator2,
"vaccine_management/vacc_mngt_indicator3"::varchar as vacc_mngt_indicator3,
"vaccine_management/vacc_mngt_indicator1"::varchar as vacc_mngt_indicator1,
"human_resource/hr_indicator1_remarks"::varchar as hr_indicator1_remarks,
"_xform_id"::varchar as "_xform_id",
"section_note"::varchar as "section_note",
"vaccine_management/v__gt_indicator2_remarks"::varchar as vacc_mngt_indicator2_remarks,
"social_mobilization/__ob_indicator2_remarks"::varchar as social_mob_indicator2_remarks,
"today"::varchar as "today",
"end"::varchar as "end",
"microplan/microplan_indicator4_remarks"::varchar as microplan_indicator4_remarks,
"immunization_safety/__ty_indicator2_remarks"::varchar as imm_safety_indicator2_remarks,
"logistics_supply/log__cs_indicator4_remarks"::varchar as logistics_indicator4_remarks,
"formhub/uuid"::varchar as uuid,
"reporting_system/reporting_indicator2"::varchar as reporting_indicator2,
"reporting_system/reporting_indicator1"::varchar as reporting_indicator1,
"supervision/supervision_indicator4_remarks"::varchar as supervision_indicator4_remarks,
"meta/instanceID"::varchar as instanceID,
"reporting_system/reporting_indicator3"::varchar as reporting_indicator3,
"start"::varchar as "start",
"reporting_system/rep__ng_indicator3_remarks"::varchar as reporting_indicator3_remarks,
"_uuid"::varchar as "_uuid",
"supervision/supervision_indicator1_remarks"::varchar as supervision_indicator1_remarks,
"logistics_supply/log__cs_indicator1_remarks"::varchar as logistics_indicator1_remarks,
"geographical_location/admin4_lat"::varchar as admin4_lat,
"microplan/microplan_indicator7_remarks"::varchar as microplan_indicator7_remarks,
"username"::varchar as "username"
FROM airbyte."sbm_health_center_le__sessment_of_readiness"
)
select
*
from data d
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
                  -- g. hc_assessment_qns_category - contains the no. of questions for each category
--- The following sections need to updated during customization:
           --a. Indicators_label unnest section: the set on questions need to be updated based on how they have been labelled within the customized Health Center Level Monitoring and Assessment of Readiness form
           --b. The admin1, admin2, admin3, admin4 and admin5 files within the airbyte schema need to be updated to match the administrative hierarchy of the respective country reporting_office adopting the tool
           --c. If the country office survey has extra categories then the no. of categories that has been used to calculate the proportion needs to be adjusted from 8 to the co no.of categories.
--NB: The question categories were derived from the groups available within the XLSForm

----The sub query below unnests the form questions
create or replace view staging.hc_assessment as 
----Unnests the questions and assigns label and categories to the respective question values. This section allows one to have a simplified long table of all the set of questions and responses within the Health Center Level Monitoring and Assessment of Readiness form
with data as
(
select 
hcl.id,
----creates the indicator name column that is useful when pulling in the indicator label and category
unnest(array['microplan_indicator1','microplan_indicator2','microplan_indicator3','microplan_indicator4','microplan_indicator5','microplan_indicator6',
'microplan_indicator7','microplan_indicator8','microplan_indicator9','logistics_indicator1','logistics_indicator2','logistics_indicator3','logistics_indicator4',
'logistics_indicator5','logistics_indicator6','logistics_indicator7','social_mob_indicator1','social_mob_indicator2','social_mob_indicator3','imm_safety_indicator1',
'imm_safety_indicator2','supervision_indicator1','supervision_indicator2','supervision_indicator3','supervision_indicator4','supervision_indicator5',
'reporting_indicator1','reporting_indicator2','reporting_indicator3','vacc_mngt_indicator1','vacc_mngt_indicator2','vacc_mngt_indicator3','vacc_mngt_indicator4','hr_indicator1']) as indicator_name,
----creates a column that has the yes/no responses of the respective questions
unnest(array[microplan_indicator1,microplan_indicator2,microplan_indicator3,microplan_indicator4,microplan_indicator5,microplan_indicator6,microplan_indicator7,
microplan_indicator8,microplan_indicator9,logistics_indicator1,logistics_indicator2,logistics_indicator3,logistics_indicator4,logistics_indicator5,logistics_indicator6,
logistics_indicator7,social_mob_indicator1,social_mob_indicator2,social_mob_indicator3,imm_safety_indicator1,imm_safety_indicator2,supervision_indicator1,supervision_indicator2,
supervision_indicator3,supervision_indicator4,supervision_indicator5,reporting_indicator1,reporting_indicator2,reporting_indicator3,vacc_mngt_indicator1,vacc_mngt_indicator2,
vacc_mngt_indicator3,vacc_mngt_indicator4,hr_indicator1]) as indicators_value, 
----creates a column that has the remarks responses of the respective questions
unnest(array[microplan_indicator1_remarks,microplan_indicator2_remarks,microplan_indicator3_remarks,microplan_indicator4_remarks,microplan_indicator5_remarks,
microplan_indicator6_remarks,microplan_indicator7_remarks,microplan_indicator8_remarks,microplan_indicator9_remarks,logistics_indicator1_remarks,logistics_indicator2_remarks,
logistics_indicator3_remarks,logistics_indicator4_remarks,logistics_indicator5_remarks,logistics_indicator6_remarks,logistics_indicator7_remarks,social_mob_indicator1_remarks,
social_mob_indicator2_remarks,social_mob_indicator3_remarks,imm_safety_indicator1_remarks,imm_safety_indicator2_remarks,supervision_indicator1_remarks,supervision_indicator2_remarks,
supervision_indicator3_remarks,supervision_indicator4_remarks,supervision_indicator5_remarks,reporting_indicator1_remarks,reporting_indicator2_remarks,reporting_indicator3_remarks,
vacc_mngt_indicator1_remarks,vacc_mngt_indicator2_remarks,vacc_mngt_indicator3_remarks,vacc_mngt_indicator4_remarks,hr_indicator2_remarks]) as indicators_remarks 
from airbyte_removed_group.hcl_monitoring_and_assessment_of_readiness hcl ---the raw table from Inform
)
select 
 d.id::bigint,
 d.indicators_value::text,
 d.indicators_remarks::text,
 i.indicator_label as indicators_label,
 i.category as indicators_category
from data d 
left join airbyte.indicators i on d.indicator_name=i.indicator_name and form_name='hcl_monitoring_and_assessment_of_readiness';---populates the indicator label and category from airbyte


create or replace view staging.hcl_monitoring_assessment as
with latest_assessment_date as ---Retrieves the latest facility assessment date 
(
select 
     admin5, max(hcl.assessment_date) as latest_assessment
from airbyte_removed_group.hcl_monitoring_and_assessment_of_readiness hcl 
group by 1 
),
latest_assessment_id as ----Retrieves the record id of the latest facility assessment
(
select  la.admin5, hc.id, la.latest_assessment
from  latest_assessment_date  la
inner join airbyte_removed_group.hcl_monitoring_and_assessment_of_readiness  hc
 on la.admin5 = hc.admin5 and la.latest_assessment = hc.assessment_date 
),
vaccine_administered as ---unnests the select multiple vaccine administered question
(
select 
   id,
   trim(unnest(string_to_array(REGEXP_REPLACE(hcl.vaccine_administered::text,'[\[\]"]', '', 'g'),' '))) as vaccine_administered 
from airbyte_removed_group.hcl_monitoring_and_assessment_of_readiness hcl
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
left join latest_assessment_id la on ha.id::varchar = la.id
left join airbyte.hc_assessment_qns_category haqpc on ha.indicators_category=haqpc.indicators_category ----The hc_assessment_questions_per_category table has the list of categories with no.of questions per category
left join airbyte_removed_group.hcl_monitoring_and_assessment_of_readiness hcl on ha.id::varchar=hcl.id 
left join airbyte.admin1 a1 on hcl.admin1=a1.name::text and ha.id::varchar=hcl.id---Adds admin 1 labels using the admin name column
left join airbyte.admin2 a2 on hcl.admin2=a2.name::text and ha.id::varchar=hcl.id ---Adds admin 2 labels using the admin name column
left join airbyte.admin3 a3 on hcl.admin3=a3.name::text and ha.id::varchar=hcl.id ---Adds admin 3 labels using the admin name column
left join airbyte.admin4 a4 on hcl.admin4=a4.name::text and ha.id::varchar=hcl.id ---Adds admin 4 labels using the admin name column
left join airbyte.admin5 a5 on hcl.admin5=a5.name::text and ha.id::varchar=hcl.id ---Adds admin 5 labels using the admin name column



----This query creates a view that has the facilities proportions based on the responses provided to the questions during the assessment
---For the query to execute successfully, the following tables are required:
                  -- a. health_center_level_monitoring_and_assessment_of_readiness - Health Center Level Monitoring and Assessment of Readiness form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
                  -- g. hc_assessment_qns_category - contains the no. of questions for each category
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

