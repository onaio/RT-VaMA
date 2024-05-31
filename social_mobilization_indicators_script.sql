----Form name: Social Mobilization Indicators
--- Objective: communication activities during or after the vaccination activity
--- Inform link: https://inform.unicef.org/uniceftemplates/635/765
---This view creates the social mobilization indicators
-- To be able to execute the queries below, you'll need access to the following tables 
                  -- a. spv_social_mobilization_indicators - this is the table that has all the responses from the spv_social_mobilization_indicators form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. spv_social_mobilization_indicators_refusals - contains refusals addressed repeat group data
                  -- g. province_iso2_codes - contains the admin 2 iso codes
--- The following section(s) need to updated during customization:
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. The unnest sections can be updated if the list of the social mobilization activities is longer than the one provided
                  --- c. province_iso2_codes within the CSV schema needs to be updated to match the admin 2 level of the reporting country office

----The Airbye connector pulls in the field names with group names appended.
---The script below removes the group names
create or replace view airbyte_removed_group.spv_social_mobilization_indicators as 
(
with data as
(
select"_xform_id_string"::varchar as  "_xform_id_string",
"soc_mob_indicators_learning_sessions"::varchar as  learning_sessions,
"soc_mob_indicators_religious_institutions"::varchar as  religious_institutions,
"remarks_grp_end_note"::varchar as  end_note,
"assessment_details_enumerator"::varchar as  enumerator,
"_review_comment"::varchar as  "_review_comment",
"_tags"::varchar as  "_tags",
"_review_status"::varchar as  "_review_status",
"soc_mob_indicators_vaccination_campaign"::varchar as  vaccination_campaign,
"soc_mob_indicators_hhs_visited"::varchar as  hhs_visited,
"_attachments"::varchar as  "_attachments",
"intro"::varchar as  "intro",
"_submission_time"::timestamp as  submitted_at,
"geographical_location_admin4"::varchar as  admin4,
"geographical_location_admin3"::varchar as  admin3,
"geographical_location_admin2"::varchar as  admin2,
"_notes"::varchar as  "_notes",
"_version"::varchar as  "_version",
"_date_modified"::timestamp as modified_at,
"_geolocation"::varchar as  "_geolocation",
"device_id"::varchar as  "device_id",
"geographical_location_admin4_long"::varchar as  admin4_long,
"_status"::varchar as  "_status",
"_media_all_received"::varchar as  "_media_all_received",
"soc_mob_indicators_advocacy_meetings"::varchar as  advocacy_meetings,
"_bamboo_dataset_id"::varchar as  "_bamboo_dataset_id",
"_edited"::varchar as  "_edited",
"remarks_grp_remarks"::varchar as  remarks,
"_id"::varchar as  id,
"soc_mob_indicators_rumors_misinformation"::varchar as  rumors_misinformation,
"soc_mob_indicators_vaccination_team"::varchar as  vaccination_team,
"soc_mob_indicators_social_mobilisers"::varchar as  social_mobilisers,
"_xform_id"::varchar as  "_xform_id",
"_total_media"::varchar as  "_total_media",
"soc_mob_indicators_posters_banners"::varchar as  posters_banners,
"soc_mob_indicators_refusals"::varchar as  refusals,
"today"::varchar as  "today",
"end"::varchar as  "end",
"_duration"::varchar as  "_duration",
"formhub_uuid"::varchar as  uuid,
"soc_mob_indicators_refusals_count"::varchar as  refusals_count,
"geographical_location_admin1"::varchar as  admin1,
"geographical_location_admin0"::varchar as  admin0,
"meta_instanceID"::varchar as  instanceID,
"remarks_grp_endnote2"::varchar as  endnote2,
"soc_mob_indicators_vaccine_administered"::varchar as  vaccine_administered,
"start"::varchar as  "start",
"_media_count"::varchar as  "_media_count",
"assessment_details_conducted_by_other"::varchar as  conducted_by_other,
"_uuid"::varchar as  "_uuid",
"_submitted_by"::varchar as  "_submitted_by",
"geographical_location_admin4_lat"::varchar as  admin4_lat,
"assessment_details_conducted_by"::varchar as  conducted_by,
"soc_mob_indicators_vaccine_related"::varchar as  vaccine_related,
"assessment_details_assessment_date"::varchar as  assessment_date,
"username"::varchar as  "username"
from airbyte_v2.sbm_spv_social_mobilization_indicators
)
select
*
from data d
);

-----Refusals addressed table from a repeat column 
create or replace view airbyte_removed_group.spv_social_mobilization_indicators_refusals as 
---This view creates the refusals addressed view
(
select 
     id,
     json_array_elements(refusals::json) ->> 'soc_mob_indicators/refusals/pos' as pos,
     json_array_elements(refusals::json) ->> 'soc_mob_indicators/refusals/vaccine_name' as vaccine_name,
     json_array_elements(refusals::json) ->> 'soc_mob_indicators/refusals/vaccine_label' as vaccine_label,
     json_array_elements(refusals::json) ->> 'soc_mob_indicators/refusals/refusals_addressed'  as refusals_addressed
from airbyte_removed_group.spv_social_mobilization_indicators ssmi
);

-----This query creates the social mobilization indicators view

create or replace view staging.social_mobilization_indicators as 
(
with rumors_misinformation as 
(
----This section unnests the select multiple option under Rumors and Misinformation
select  
 id,
 unnest(array['Vaccine related','Vaccination team','Vaccination campaign']) as indicators_category,
 unnest(array[vaccine_related,vaccination_team,vaccination_campaign]) as indicators_value
from airbyte_removed_group.spv_social_mobilization_indicators
),
mobilization_indicators as
(
--This section creates a tidy section of the social mobilization activities such as number of households visited etc.
select 
 id,
 unnest(array['Social mobilisers/community volunteers engaged','Households visited','Group meetings/learning sessions with caregivers conducted',
 'Religious institutions visited','Advocacy meetings with community leaders','Posters and banners produced and displayed']) as indicator_category,
 unnest(array[social_mobilisers,hhs_visited,learning_sessions,religious_institutions,advocacy_meetings,posters_banners]) as indicator_value
from airbyte_removed_group.spv_social_mobilization_indicators
),
----This section creates a tidy section of the refusals addressed from the spv_social_mobilization_indicators_refusals repeat group data table
refusals_addressed as
(
select 
     id,
     'Refusals addressed' as indicator_category,
     json_array_elements(refusals::json) ->> 'soc_mob_indicators/refusals/refusals_addressed'  as indicator_value
from airbyte_removed_group.spv_social_mobilization_indicators ssmi
),
----This section unions all the subsections above
combined_indicators as 
(
select * from rumors_misinformation
union all 
select * from mobilization_indicators
union all 
select * from refusals_addressed
)
select
  c.id::bigint, 
  ssmi.assessment_date::date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  case 
  	when ssmi.conducted_by='others' then conducted_by_other else conducted_by::text 
  end as conducted_by,  
  c.indicators_category,
  c.indicators_value::bigint,
  ssmir.vaccine_label as vaccine_administered,
  pic.iso2_code::varchar(50),
  ssmi.submitted_at::timestamp as submitted_at,
  ssmi.modified_at::timestamp as modified_at,
  ssmi.enumerator::text 
from combined_indicators c  
left join airbyte_removed_group.spv_social_mobilization_indicators ssmi on c.id=ssmi.id
left join airbyte_v2.admin1 a1 on ssmi.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join airbyte_v2.admin2 a2 on ssmi.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join airbyte_v2.admin3 a3 on ssmi.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join airbyte_v2.admin4 a4 on ssmi.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join airbyte_removed_group.spv_social_mobilization_indicators_refusals ssmir on ssmir.id=c.id
left join airbyte_v2.province_iso2_codes pic on pic.admin2_id=a2.name
);



-----POWER BI VIEWS
---The PowerBI connector currently pulls data from views within the public schema. Hence the repointing to the public schema
create or replace view public.social_mobilization_indicators as 
(
select  
    id,
    assessment_date,
    admin1,
    admin2,
    admin3,
    admin4,
    conducted_by,
    indicators_category,
    indicators_value,
    vaccine_administered,
    iso2_code,
    submitted_at::date,
    modified_at::date,
    enumerator
from staging.social_mobilization_indicators
);
