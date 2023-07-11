 ---Form name: Synchronized Vaccination Monitoring Tool
 ---Objective: Monitors the status of the health facility
----Inform link: https://inform.unicef.org/unicefairbyte_removed_group/635/847
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
                  -- h. synchronized_monitoring_tool airbyte file - contains the groups and the no. of questions within each group. This file is useful when calculating the proportion of each group based on the yes responses provided on the questions within the group
--- The following section(s) need to updated during customization:
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the airbyte schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. The unnest sections can be updated if the list of questions and groups in the XLSForm is longer than the one provided
                  --- c. monitoring_tool_qns_category airbyte file can be updated if the no. of questions and list of groups in the XLSForm are more than the ones within the file

---The query below removes group names from the Airbyte monitoring tool form
create or replace view airbyte_removed_group.synchronized_vaccination_monitoring_tool as
(
with data as
(
select
 "_xform_id_string"::varchar as "_xform_id_string",
"part2/part2_indicator14_remarks"::varchar as part2_indicator14_remarks,
"remarks_grp/end_note"::varchar as end_note,
"_review_comment"::varchar as "_review_comment",
"part1/part1_indicator6_remarks"::varchar as part1_indicator6_remarks,
"part2/part2_indicator10_remarks"::varchar as part2_indicator10_remarks,
"immunization_coverage/vaccine_label"::varchar as vaccine_label,
"_attachments"::varchar as "_attachments",
"part2/part2_indicator27_remarks"::varchar as part2_indicator27_remarks,
"_submission_time"::date as submitted_at,
"part1/part1_indicator22_remarks"::varchar as part1_indicator22_remarks,
"_version"::varchar as "_version",
"_date_modified"::date as modified_at,
"part1/part1_indicator17_remarks"::varchar as part1_indicator17_remarks,
"campaign_activity_de___vaccination_activity"::varchar as time_vaccination_activity,
"device_id"::varchar as "device_id",
"_status"::varchar as "_status",
"part2/part2_indicator23_remarks"::varchar as part2_indicator23_remarks,
"_media_all_received"::varchar as "_media_all_received",
"_edited"::varchar as "_edited",
"part1/part1_indicator13_remarks"::varchar as part1_indicator13_remarks,
"remarks_grp/remarks"::varchar as remarks,
"part2/part2_indicator3_remarks"::varchar as part2_indicator3_remarks,
"campaign_activity_details/enumerator"::varchar as enumerator,
"part2/part2_indicator18_remarks"::varchar as part2_indicator18_remarks,
"part1/part1_indicator3_remarks"::varchar as part1_indicator3_remarks,
"part1/part1_indicator9_remarks"::varchar as part1_indicator9_remarks,
"part2/part2_indicator29"::varchar as part2_indicator29,
"part2/part2_indicator28"::varchar as part2_indicator28,
"part2/part2_indicator27"::varchar as part2_indicator27,
"part2/part2_indicator26"::varchar as part2_indicator26,
"part2/part2_indicator25"::varchar as part2_indicator25,
"part2/part2_indicator9_remarks"::varchar as part2_indicator9_remarks,
"part2/part2_indicator24"::varchar as part2_indicator24,
"part2/part2_indicator23"::varchar as part2_indicator23,
"_total_media"::varchar as "_total_media",
"part2/part2_indicator28a"::varchar as part2_indicator28a,
"part2/part2_indicator15_remarks"::varchar as part2_indicator15_remarks,
"part2/part2_indicator28b"::varchar as part2_indicator28b,
"part2/part2_indicator11_remarks"::varchar as part2_indicator11_remarks,
"part1/part1_indicator21_remarks"::varchar as part1_indicator21_remarks,
"part2/part2_indicator28_remarks"::varchar as part2_indicator28_remarks,
"part1/part1_indicator19_remarks"::varchar as part1_indicator19_remarks,
"_duration"::varchar as "_duration",
"part2/part2_indicator30"::varchar as part2_indicator30,
"part2/part2_indicator9"::varchar as part2_indicator9,
"part2/part2_indicator7"::varchar as part2_indicator7,
"campaign_activity_details/reporting_office"::varchar as reporting_office,
"part2/part2_indicator8"::varchar as part2_indicator8,
"geographical_location/admin1"::varchar as admin1,
"geographical_location/admin0"::varchar as admin0,
"part1/part1_indicator16_remarks"::varchar as part1_indicator16_remarks,
"remarks_grp/endnote2"::varchar as endnote2,
"_media_count"::varchar as "_media_count",
"part2/part2_indicator1"::varchar as part2_indicator1,
"part2/part2_indicator2"::varchar as part2_indicator2,
"part3/vaccination_team_info_count"::varchar as vaccination_team_info_count,
"_submitted_by"::varchar as "_submitted_by",
"part2/part2_indicator2_remarks"::varchar as part2_indicator2_remarks,
"part2/part2_indicator24_remarks"::varchar as part2_indicator24_remarks,
"part2/part2_indicator5"::varchar as part2_indicator5,
"part2/part2_indicator6"::varchar as part2_indicator6,
"part2/part2_indicator3"::varchar as part2_indicator3,
"part2/part2_indicator4"::varchar as part2_indicator4,
"part2/part2_indicator19_remarks"::varchar as part2_indicator19_remarks,
"part1/part1_indicator12_remarks"::varchar as part1_indicator12_remarks,
"part2/part2_indicator30b"::varchar as part2_indicator30b,
"part2/part2_indicator30c"::varchar as part2_indicator30c,
"part2/part2_indicator6_remarks"::varchar as part2_indicator6_remarks,
"part2/part2_indicator30a"::varchar as part2_indicator30a,
"part1/part1_indicator2_remarks"::varchar as part1_indicator2_remarks,
"part2/part2_indicator20_remarks"::varchar as part2_indicator20_remarks,
"part1/part1_indicator8_remarks"::varchar as part1_indicator8_remarks,
"part1/part1_indicator12"::varchar as part1_indicator12,
"part1/part1_indicator13"::varchar as part1_indicator13,
"part2/part2_indicator16_remarks"::varchar as part2_indicator16_remarks,
"part1/part1_indicator10"::varchar as part1_indicator10,
"part1/part1_indicator11"::varchar as part1_indicator11,
"part1/part1_indicator24_remarks"::varchar as part1_indicator24_remarks,
"_tags"::varchar as "_tags",
"_review_status"::varchar as "_review_status",
"part2/part2_indicator8_remarks"::varchar as part2_indicator8_remarks,
"part1/part1_indicator11_remarks"::varchar as part1_indicator11_remarks,
"part1/part1_indicator20_remarks"::varchar as part1_indicator20_remarks,
"part2/part2_indicator29_remarks"::varchar as part2_indicator29_remarks,
"part2/part2_indicator25_remarks"::varchar as part2_indicator25_remarks,
"intro"::varchar as "intro",
"part2/part2_indicator11"::varchar as part2_indicator11,
"geographical_location/admin5"::varchar as admin5,
"part2/part2_indicator10"::varchar as part2_indicator10,
"geographical_location/admin4"::varchar as admin4,
"geographical_location/admin3"::varchar as admin3,
"immunization_coverage/hc_target"::varchar as hc_target,
"geographical_location/admin2"::varchar as admin2,
"_notes"::varchar as "_notes",
"immunization_coverage/vaccine_administered"::varchar as vaccine_administered,
"geographical_location/admin6"::varchar as admin6,
"part2/part2_indicator12_remarks"::varchar as part2_indicator12_remarks,
"part1/part1_indicator3"::varchar as part1_indicator3,
"part2/part2_indicator19"::varchar as part2_indicator19,
"part1/part1_indicator4"::varchar as part1_indicator4,
"part1/part1_indicator20"::varchar as part1_indicator20,
"part2/part2_indicator18"::varchar as part2_indicator18,
"_geolocation"::varchar as "_geolocation",
"part1/part1_indicator1"::varchar as part1_indicator1,
"part2/part2_indicator17"::varchar as part2_indicator17,
"part2/part2_indicator21_remarks"::varchar as part2_indicator21_remarks,
"part1/part1_indicator2"::varchar as part1_indicator2,
"part2/part2_indicator16"::varchar as part2_indicator16,
"part1/part1_indicator23"::varchar as part1_indicator23,
"part2/part2_indicator15"::varchar as part2_indicator15,
"part1/part1_indicator24"::varchar as part1_indicator24,
"part2/part2_indicator14"::varchar as part2_indicator14,
"part1/part1_indicator1_remarks"::varchar as part1_indicator1_remarks,
"part1/part1_indicator21"::varchar as part1_indicator21,
"part2/part2_indicator13"::varchar as part2_indicator13,
"part1/part1_indicator22"::varchar as part1_indicator22,
"part2/part2_indicator12"::varchar as part2_indicator12,
"immunization_coverage/vaccination_teams_no"::int as vaccination_teams_no,
"immunization_coverag__cinated_as_time_visit"::int as total_vaccinated_as_time_visit,
"part2/part2_indicator29a"::varchar as part2_indicator29a,
"geographical_location/admin4_long"::real as admin4_long,
"part1/part1_indicator9"::varchar as part1_indicator9,
"part1/part1_indicator7"::varchar as part1_indicator7,
"part1/part1_indicator8"::varchar as part1_indicator8,
"part1/part1_indicator5"::varchar as part1_indicator5,
"part2/part2_indicator29b"::varchar as part2_indicator29b,
"part2/part2_indicator1_remarks"::varchar as part2_indicator1_remarks,
"part1/part1_indicator6"::varchar as part1_indicator6,
"part1/part1_indicator5_remarks"::varchar as part1_indicator5_remarks,
"_bamboo_dataset_id"::varchar as "_bamboo_dataset_id",
"part2/part2_indicator5_remarks"::varchar as part2_indicator5_remarks,
"part1/part1_indicator16"::varchar as part1_indicator16,
"part2/part2_indicator22"::varchar as part2_indicator22,
"part1/part1_indicator17"::varchar as part1_indicator17,
"part2/part2_indicator21"::varchar as part2_indicator21,
"part1/part1_indicator14"::varchar as part1_indicator14,
"part2/part2_indicator20"::varchar as part2_indicator20,
"part1/part1_indicator15_remarks"::varchar as part1_indicator15_remarks,
"_id"::varchar as id,
"part1/part1_indicator15"::varchar as part1_indicator15,
"campaign_activity_de__accination_activity_1"::date as date_vaccination_activity,
"part1/part1_indicator18"::varchar as part1_indicator18,
"part1/part1_indicator19"::varchar as part1_indicator19,
"part2/part2_indicator13_remarks"::varchar as part2_indicator13_remarks,
"part2/part2_indicator30_remarks"::varchar as part2_indicator30_remarks,
"part1/part1_indicator10_remarks"::varchar as part1_indicator10_remarks,
"part1/part1_indicator7_remarks"::varchar as part1_indicator7_remarks,
"_xform_id"::varchar as "_xform_id",
"part2/part2_indicator7_remarks"::varchar as part2_indicator7_remarks,
"part2/part2_indicator26_remarks"::varchar as part2_indicator26_remarks,
"today"::date as "today",
"end"::timestamp as "end",
"formhub/uuid"::varchar as uuid,
"part1/part1_indicator23_remarks"::varchar as part1_indicator23_remarks,
"part1/part1_indicator18_remarks"::varchar as part1_indicator18_remarks,
"meta/instanceID"::varchar as instanceID,
"part2/part2_indicator22_remarks"::varchar as part2_indicator22_remarks,
"start"::timestamp as "start",
"_uuid"::varchar as "_uuid",
"geographical_location/admin4_lat"::real as admin4_lat,
"part1/part1_indicator4_remarks"::varchar as part1_indicator4_remarks,
"part3/vaccination_team_info"::varchar as vaccination_team_info,
"part2/part2_indicator17_remarks"::varchar as part2_indicator17_remarks,
"part2/part2_indicator4_remarks"::varchar as part2_indicator4_remarks,
"part1/part1_indicator14_remarks"::varchar as part1_indicator14_remarks,
"username"::varchar as "username",
"_airbyte_ab_id"::varchar as "_airbyte_ab_id",
"_airbyte_emitted_at"::varchar as "_airbyte_emitted_at",
"_airbyte_normalized_at"::varchar as "_airbyte_normalized_at",
"_airbyte_sbm_synchro__onitoring_tool_hashid"::varchar as
"_airbyte_sbm_synchro__onitoring_tool_hashid"
from airbyte.sbm_synchronized_vac__ation_monitoring_tool 
)
select 
* 
from data
); 
 
----The query below removes group name from the vaccination team repeat group fields
create or replace view airbyte_removed_group.synchronized_vaccination_monitoring_tool_vaccination_team_info as 
(
with
data as
(
select
"_airbyte_sbm_synchro__onitoring_tool_hashid"::varchar as "_airbyte_sbm_monitoring_tool_hashid",
"part3/vaccination_te__t3_indicator4_remarks"::varchar as part3_indicator4_remarks,
"part3/vaccination_te__t3_indicator2_remarks"::varchar as part3_indicator2_remarks,
"part3/vaccination_te__t3_indicator5_remarks"::varchar as part3_indicator5_remarks,
"part3/vaccination_te__t3_indicator1_remarks"::varchar as part3_indicator1_remarks,
"part3/vaccination_te__t3_indicator6_remarks"::varchar as part3_indicator6_remarks,
"part3/vaccination_te__t3_indicator7_remarks"::varchar as part3_indicator7_remarks,
"part3/vaccination_te__t3_indicator8_remarks"::varchar as part3_indicator8_remarks,
"part3/vaccination_te__3_indicator14_remarks"::varchar as part3_indicator14_remarks,
"part3/vaccination_te__3_indicator13_remarks"::varchar as part3_indicator13_remarks,
"part3/vaccination_te__nfo/part3_indicator7b"::varchar as part3_indicator7b,
"part3/vaccination_te__nfo/part3_indicator7a"::varchar as part3_indicator7a,
"part3/vaccination_te__3_indicator12_remarks"::varchar as part3_indicator12_remarks,
"part3/vaccination_team_info/note"::varchar as note,
"part3/vaccination_te__t3_indicator9_remarks"::varchar as part3_indicator9_remarks,
"part3/vaccination_te__nfo/part3_indicator12"::varchar as part3_indicator12,
"part3/vaccination_te__nfo/part3_indicator11"::varchar as part3_indicator11,
"part3/vaccination_te__3_indicator11_remarks"::varchar as part3_indicator11_remarks,
"part3/vaccination_te__nfo/part3_indicator14"::varchar as part3_indicator14,
"part3/vaccination_te__nfo/part3_indicator13"::varchar as part3_indicator13,
"part3/vaccination_te__3_indicator10_remarks"::varchar as part3_indicator10_remarks,
"part3/vaccination_te__nfo/part3_indicator16"::varchar as part3_indicator16,
"part3/vaccination_te__nfo/part3_indicator15"::varchar as part3_indicator15,
"part3/vaccination_te__nfo/part3_indicator18"::varchar as part3_indicator18,
"part3/vaccination_te__nfo/part3_indicator17"::varchar as part3_indicator17,
"part3/vaccination_te__info/part3_indicator6"::varchar as part3_indicator6,
"part3/vaccination_te__info/part3_indicator7"::varchar as part3_indicator7,
"part3/vaccination_te__info/part3_indicator4"::varchar as part3_indicator4,
"part3/vaccination_te__info/part3_indicator5"::varchar as part3_indicator5,
"part3/vaccination_te__info/part3_indicator2"::varchar as part3_indicator2,
"part3/vaccination_te__info/part3_indicator3"::varchar as part3_indicator3,
"part3/vaccination_te__nfo/part3_indicator10"::varchar as part3_indicator10,
"part3/vaccination_te__3_indicator15_remarks"::varchar as part3_indicator15_remarks,
"part3/vaccination_te__info/part3_indicator1"::varchar as part3_indicator1,
"part3/vaccination_te__3_indicator16_remarks"::varchar as part3_indicator16_remarks,
"part3/vaccination_team_info/pos"::varchar as pos,
"part3/vaccination_te__3_indicator17_remarks"::varchar as part3_indicator17_remarks,
"part3/vaccination_te__info/part3_indicator8"::varchar as part3_indicator8,
"part3/vaccination_te__info/part3_indicator9"::varchar as part3_indicator9,
"part3/vaccination_team_info/vt_leader"::varchar as vt_leader,
"part3/vaccination_te__fo/part3_indicator12a"::varchar as part3_indicator12a,
"part3/vaccination_te__t3_indicator3_remarks"::varchar as part3_indicator3_remarks,
"part3/vaccination_te__3_indicator18_remarks"::varchar as part3_indicator18_remarks,
"_airbyte_ab_id"::varchar as "_airbyte_ab_id",
"_airbyte_emitted_at"::varchar as "_airbyte_emitted_at",
"_airbyte_normalized_at"::varchar as "_airbyte_normalized_at",
"_airbyte_part3_vaccination_team_info_hashid"::varchar as
"_airbyte_part3_vaccination_team_info_hashid"
from airbyte.sbm_synchronized_vac__vaccination_team_info
)
select
*
from data
); 
 -----The sub query below creates a view that only has the microplan and vaccine management questions
 create or replace view staging.microplan_vaccine_management as 
(
with data as 
(
select 
  svmt.id,
  unnest(array['part1_indicator1','part1_indicator2','part1_indicator3','part1_indicator4','part1_indicator5','part1_indicator6','part1_indicator7','part1_indicator8',
  'part1_indicator9','part1_indicator10','part1_indicator11','part1_indicator12','part1_indicator13','part1_indicator14','part1_indicator15','part1_indicator16','part1_indicator17',
  'part1_indicator18','part1_indicator19','part1_indicator20','part1_indicator21','part1_indicator22','part1_indicator23','part1_indicator24','part2_indicator1','part2_indicator2',
  'part2_indicator3','part2_indicator4','part2_indicator5','part2_indicator6','part2_indicator7','part2_indicator8','part2_indicator9','part2_indicator10','part2_indicator11',
  'part2_indicator12','part2_indicator13','part2_indicator14','part2_indicator15','part2_indicator16','part2_indicator17','part2_indicator18','part2_indicator19','part2_indicator20',
  'part2_indicator21','part2_indicator22','part2_indicator23','part2_indicator24','part2_indicator25','part2_indicator26','part2_indicator27','part2_indicator28','part2_indicator29',
  'part2_indicator30','part2_indicator28a','part2_indicator29a','part2_indicator30a']) as indicator_name,
  unnest(array[part1_indicator1,part1_indicator2,part1_indicator3,part1_indicator4,part1_indicator5,part1_indicator6,part1_indicator7,part1_indicator8,part1_indicator9,
  part1_indicator10,part1_indicator11,part1_indicator12,part1_indicator13,part1_indicator14,part1_indicator15,part1_indicator16,part1_indicator17,part1_indicator18,
  part1_indicator19,part1_indicator20,part1_indicator21,part1_indicator22,part1_indicator23,part1_indicator24,part2_indicator1,part2_indicator2,part2_indicator3,
  part2_indicator4,part2_indicator5,part2_indicator6,part2_indicator7,part2_indicator8,part2_indicator9,part2_indicator10,part2_indicator11,part2_indicator12,
  part2_indicator13,part2_indicator14,part2_indicator15,part2_indicator16,part2_indicator17,part2_indicator18,part2_indicator19,part2_indicator20,part2_indicator21,
  part2_indicator22,part2_indicator23,part2_indicator24,part2_indicator25,part2_indicator26,part2_indicator27,part2_indicator28,part2_indicator29,part2_indicator30,
  part2_indicator28a::text,part2_indicator29a::text,part2_indicator30a::text]) as indicators_value,
  unnest(array[part1_indicator1_remarks,part1_indicator2_remarks,part1_indicator3_remarks,part1_indicator4_remarks,part1_indicator5_remarks,part1_indicator6_remarks,
  part1_indicator7_remarks,part1_indicator8_remarks,part1_indicator9_remarks,part1_indicator10_remarks,part1_indicator11_remarks,part1_indicator12_remarks,
  part1_indicator13_remarks,part1_indicator14_remarks,part1_indicator15_remarks,part1_indicator16_remarks,part1_indicator17_remarks,part1_indicator18_remarks,
  part1_indicator19_remarks,part1_indicator20_remarks,part1_indicator21_remarks,part1_indicator22_remarks,part1_indicator23_remarks,part1_indicator24_remarks,
  part2_indicator1_remarks,part2_indicator2_remarks,part2_indicator3_remarks,part2_indicator4_remarks,part2_indicator5_remarks,part2_indicator6_remarks,
  part2_indicator7_remarks,part2_indicator8_remarks,part2_indicator9_remarks,part2_indicator10_remarks,part2_indicator11_remarks,part2_indicator12_remarks,
  part2_indicator13_remarks,part2_indicator14_remarks,part2_indicator15_remarks,part2_indicator16_remarks,part2_indicator17_remarks,part2_indicator18_remarks,
  part2_indicator19_remarks,part2_indicator20_remarks,part2_indicator21_remarks,part2_indicator22_remarks,part2_indicator23_remarks,part2_indicator24_remarks,
  part2_indicator25_remarks,part2_indicator26_remarks,part2_indicator27_remarks,part2_indicator28_remarks,part2_indicator29_remarks,part2_indicator30_remarks,
  part2_indicator28b,part2_indicator29b,part2_indicator30b]) as indicators_remarks
from airbyte_removed_group.synchronized_vaccination_monitoring_tool svmt
)
select 
   d.id::bigint,
   d.indicators_value::text,
   d.indicators_remarks::text,
   i.indicator_label as indicators_label,
   i.category as indicators_category
from data d
left join airbyte.indicators i on d.indicator_name=i.indicator_name AND form_name='synchronized_vaccination_monitoring_tool'
);


----The sub query below creates a view that has the vaccination site questions
create or replace view staging.vaccination_site_questions_monitoring_tool as 
(
with data as
(
select 
  "_airbyte_ab_id",
  unnest(array['part3_indicator1','part3_indicator2','part3_indicator3','part3_indicator4','part3_indicator5','part3_indicator6','part3_indicator7','part3_indicator8',
  'part3_indicator9','part3_indicator10','part3_indicator11','part3_indicator12','part3_indicator13','part3_indicator14','part3_indicator15','part3_indicator16',
  'part3_indicator17','part3_indicator18'])as indicator_name,
  unnest(array[part3_indicator1,part3_indicator2,part3_indicator3,part3_indicator4,part3_indicator5,part3_indicator6,part3_indicator7,part3_indicator8,part3_indicator9,
  part3_indicator10,part3_indicator11,part3_indicator12,part3_indicator13,part3_indicator14,part3_indicator15,part3_indicator16,part3_indicator17,part3_indicator18])
  as indicators_value,
  unnest(array[part3_indicator1_remarks,part3_indicator2_remarks,part3_indicator3_remarks,part3_indicator4_remarks,part3_indicator5_remarks,part3_indicator6_remarks,
  part3_indicator7_remarks,part3_indicator8_remarks,part3_indicator9_remarks,part3_indicator10_remarks,part3_indicator11_remarks,part3_indicator12_remarks,
  part3_indicator13_remarks,part3_indicator14_remarks,part3_indicator15_remarks,part3_indicator16_remarks,part3_indicator17_remarks,part3_indicator18_remarks])
  as indicators_remarks
from airbyte_removed_group.synchronized_vaccination_monitoring_tool_vaccination_team_info svmtvti  
)
select 
   svmt.id::bigint as parent_id,
   d.indicators_value::text,
   d.indicators_remarks::text,
   i.indicator_label as indicators_label,
   i.category as indicators_category 
from data d
left join airbyte_removed_group.synchronized_vaccination_monitoring_tool svmt on d._airbyte_ab_id=svmt._airbyte_ab_id
left join airbyte.indicators i on d.indicator_name=i.indicator_name AND form_name='synchronized_vaccination_monitoring_tool'
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
  svmt.vaccine_label::text, 
  mt.indicators_value,
  mt.indicators_remarks, 
  mt.indicators_label,
  mt.indicators_category,
  smt.no_of_questions::int  
from monitoring_tool mt
left join airbyte.monitoring_tool_qns_category smt on mt.indicators_category=smt.indicators_category ---adds no.of questions in the category
left join airbyte_removed_group.synchronized_vaccination_monitoring_tool svmt on mt.id=svmt.id::bigint ---adds the missing fields in the repeat group
left join airbyte.admin1 a1 on svmt.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join airbyte.admin2 a2 on svmt.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join airbyte.admin3 a3 on svmt.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join airbyte.admin4 a4 on svmt.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join airbyte.admin5 a5 on svmt.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
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
                  -- h. synchronized_monitoring_tool airbyte file - contains the groups and the no. of questions within each group. This file is useful when calculating the proportion of each group based on the yes responses provided on the questions within the group

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
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the airbyte schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. province_iso2_codes needs to be updated to match the admin 2 iso codes of the reporting country office
create or replace view staging.vaccinated_children_under_monitoring_tool as
(
select  
svmt.id::bigint,
a1.label as admin1,
a2.label as admin2,
a3.label as admin3,
a4.label as admin4,
svmt.admin4_lat::real as latitude,
svmt.admin4_long::real as longitude,
a5.label as admin5,
svmt.date_vaccination_activity,
svmt.vaccine_label::text as vaccine_administered,
svmt.total_vaccinated_as_time_visit::bigint,
svmt.hc_target::bigint,
pic.iso2_code::varchar,
svmt.submitted_at,
svmt.modified_at,
svmt.enumerator
from airbyte_removed_group.synchronized_vaccination_monitoring_tool svmt 
left join airbyte.admin1 a1 on svmt.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join airbyte.admin2 a2 on svmt.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join airbyte.admin3 a3 on svmt.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join airbyte.admin4 a4 on svmt.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join airbyte.admin5 a5 on svmt.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join airbyte.province_iso2_codes pic on pic.admin2_id::text=a2.name::text ---
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

