----Form name: Rapid Convenience Assessment
--- Objective: To asssess whether the vaccination campaign has reached its target
--- Inform link :https://inform.unicef.org/uniceftemplates/635/762
--- To be able to execute the queries below, you'll need access to the following tables 
                  -- a. spv_rapid_coverage_assessment_form - this is the table that has all the responses from the Rapid Coverage Assessment form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
                  -- g. spv_rapid_coverage_assessment_form_vaccination - the table that has the households information on the no. of present in the hh, no.of vaccinate/not vaccinated children

---Labels view
--- When pulling data from Inform, the connector separates the labels from the actual values. Labels are normally defined on the choices worksheet of the XLSForm
--- This view extracts labels from the airbyte choices table
create or replace view airbyte_removed_group.rca_labels as 
(
select
    field as question,
    value as code,
    label as label
from airbyte_v2.chc_spv_rapid_coverage_assessment_form csrcaf 
);

----This query removes group names from the Airbyte connector table 
---S.M (31.05.24) Some Airbyte relevant columns are not included in this version
create or replace view airbyte_removed_group.spv_rapid_coverage_assessment_form as 

(
with data as
(
select
"_xform_id_string"::varchar as "_xform_id_string",
"rca_mop_up"::varchar as "rca_mop_up",
"remarks_grp_end_note"::varchar as end_note,
"assessment_details_enumerator"::varchar as enumerator,
"_review_comment"::varchar as "_review_comment",
"_tags"::varchar as "_tags",
"immunization_coverage_vaccination_count"::varchar as vaccination_count,
"_review_status"::varchar as "_review_status",
"immunization_coverage_vaccination"::varchar as vaccination,
"_attachments"::varchar as "_attachments",
"assessment_details_team_members"::varchar as team_members,
"sources_health_workers"::varchar as health_workers,
"intro"::varchar as "intro",
"_submission_time"::timestamp as submitted_at,
"geographical_location_admin5"::varchar as admin5,
"geographical_location_admin4"::varchar as admin4,
"sources_radio"::int as radio,
"geographical_location_admin3"::varchar as admin3,
"geographical_location_admin2"::varchar as admin2,
"_notes"::varchar as "_notes",
"_version"::varchar as "_version",
"immunization_coverage_vaccine_administered"::varchar as vaccine_administered,
"_date_modified"::timestamp as modified_at,
"sources_sources_total"::int as sources_total,
"_geolocation"::varchar as "_geolocation",
"device_id"::varchar as "device_id",
"geographical_location_admin4_long"::varchar as admin4_long,
"_status"::varchar as "_status",
"_media_all_received"::varchar as "_media_all_received",
"sources_relatives_neighbours"::int as relatives_neighbours,
"_bamboo_dataset_id"::varchar as "_bamboo_dataset_id",
"_edited"::varchar as "_edited",
"sources_sources_note"::varchar as sources_note,
"sources_social_media"::int as social_media,
"remarks_grp_remarks"::varchar as remarks,
"_id"::varchar as id,
"sources_streamer"::int as streamer,
"_xform_id"::varchar as "_xform_id",
"_total_media"::varchar as "_total_media",
"sources_flyers"::int as flyers,
"sources_officials"::int as officials,
"sources_others"::int as others,
"immunization_coverage_doors_visited"::int as doors_visited,
"today"::varchar as "today",
"end"::varchar as "end",
"_duration"::varchar as "_duration",
"formhub_uuid"::varchar as uuid,
"sources_source_info"::varchar as source_info,
"geographical_location_admin1"::varchar as admin1,
"geographical_location_admin0"::varchar as admin0,
"meta_instanceID"::varchar as instanceID,
"remarks_grp_endnote2"::varchar as endnote2,
"start"::varchar as "start",
"_media_count"::varchar as "_media_count",
"assessment_details_conducted_by_other"::varchar as conducted_by_other,
"sources_source_info_other"::varchar as source_info_other,
"_uuid"::varchar as "_uuid",
"_submitted_by"::varchar as "_submitted_by",
"assessment_details_rca_date"::varchar as rca_date,
"immunization_coverage_vaccinated_total"::int as vaccinated_total,
"immunization_coverage_age_group"::varchar as age_group,
"geographical_location_admin4_lat"::varchar as admin4_lat,
"sources_tv"::int as tv,
"immunization_coverage_total_unvaccinated"::int as total_unvaccinated,
"assessment_details_conducted_by"::varchar as conducted_by,
"username"::varchar as "username",
"_airbyte_raw_id"::varchar as "_airbyte_ab_id",
"_airbyte_extracted_at"::varchar as "_airbyte_emitted_at"
--"_airbyte_normalized_at"::varchar as "_airbyte_normalized_at",
--"_airbyte_sbm_spv_rap__ssessment_form_hashid"::varchar as "_airbyte_sbm_spv_rap__ssessment_form_hashid"
from airbyte_v2.sbm_spv_rapid_coverage_assessment_form
)
select
*
from data
);

----This query creates a view that has the group names removed from data within the repeat group
create or replace view airbyte_removed_group.spv_rapid_coverage_assessment_form_vaccination as 
(
with data as
(
select
--"_airbyte_sbm_spv_rap__ssessment_form_hashid" as "_airbyte_sbm_spv_rapid_assessment_form_hashid",
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reason_10' as reason_10,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/not_vaccinated_males' as not_vaccinated_males,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/present_males' as present_males,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/not_vaccinated_females' as not_vaccinated_females,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/females_remain' as females_remain,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/total_not_vaccinated' as total_not_vaccinated,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reasons' as reasons,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/age_group_name' as age_group_name,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/present_females' as present_females,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/age_group_label' as age_group_label,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/pos' as pos,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reasons_total' as reasons_total,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reason_1'as reason_1,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reason_2' as reason_2,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/finger_marked_females' as finger_marked_females,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reasons_other' as reasons_other,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reason_9' as reason_9,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reason_7' as reason_7,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/finger_marked_males' as finger_marked_males,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reason_8' as reason_8,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reason_5' as reason_5,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reason_6' as reason_6,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reason_3' as reason_3,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reason_4' as reason_4,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/other_reasons' as other_reasons,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/unvaccinated/reasons_note' as reasons_note,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/males_remain' as males_remain,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/total_vaccinated' as total_vaccinated,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/vaccinated_males' as vaccinated_males,
jsonb_array_elements(immunization_coverage_vaccination) ->> 'immunization_coverage/vaccination/vaccinated_females' as vaccinated_females,
"_airbyte_raw_id"::varchar as "_airbyte_ab_id",
"_airbyte_extracted_at"::varchar as "_airbyte_emitted_at"
--"_airbyte_normalized_at"::varchar as "_airbyte_normalized_at",
--"_airbyte_immunizatio__ge_vaccination_hashid"::varchar as "_airbyte_immunization_coverage_vaccination_hashid"
from airbyte_v2.sbm_spv_rapid_coverage_assessment_form
)
select
*
from data
);

----Tidy table
---This query creates a tidy table of the RCA form actual values ie children present in the hh, vaccinated children and not vaccinated children
---For the query to execute successfully, the following tables are required:
                  -- a. spv_rapid_coverage_assessment_form - this is the table that has all the responses from the Rapid Coverage Assessment form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
                  -- g. spv_rapid_coverage_assessment_form_vaccination - the table that has the households information on the no. of present in the hh, no.of vaccinate/not vaccinated children
--- The following section(s) need to be updated during customization:
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. province_iso2_codes needs to be updated to match the reporting country office iso2 codes
create or replace view staging.rca_actuals as
(
select 
  srcaf.id::bigint as submission_id,
  srcaf.rca_date::date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  srcaf.admin4_lat::real as latitude,
  srcaf.admin4_long::real as longitude,
  a5.label as admin5,
  rl.label as vaccine_administered,
  srcafv.age_group_label::text,
  unnest(array['Present in hh','Present in hh','Vaccinated','Vaccinated','Not vaccinated','Not vaccinated']) as rca_category, ----creates a column that has the list provided,
  unnest(array['Males','Females','Males','Females','Males','Females']) as indicator_category,  ---- creates a column that has the gender list,
  (unnest(array[srcafv.present_males,srcafv.present_females,srcafv.vaccinated_males,srcafv.vaccinated_females,srcafv.not_vaccinated_males,srcafv.not_vaccinated_females]))::bigint as indicator_value, ----creates a column that has the actual values of the fields specified,
  pic.iso2_code::varchar(50)
from airbyte_removed_group.spv_rapid_coverage_assessment_form_vaccination srcafv 
left join airbyte_removed_group.spv_rapid_coverage_assessment_form srcaf  on srcafv._airbyte_ab_id=srcaf._airbyte_ab_id  --- Adds the fields associated to the repeat group data
left join airbyte_v2.admin1 a1 on srcaf.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join airbyte_v2.admin2 a2 on srcaf.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join airbyte_v2.admin3 a3 on srcaf.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join airbyte_v2.admin4 a4 on srcaf.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join airbyte_v2.admin5 a5 on srcaf.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join airbyte_removed_group.rca_labels rl on rl.code=srcaf.vaccine_administered and rl.question='vaccine_administered' ---adds the vaccine labels from the labels table
left join airbyte_v2.province_iso2_codes pic on pic.admin2_id::text=a2.name::text ---adds the admin 2 isocodes to be used for the country maps on superset
);


----This query creates a view that has the no.of doors visited, whether mop-up is needed and number of children not vaccinated in the doors visited
--- To be able to perform a sum of the doors visited and no. of children not vaccinated within the visuals this view had to be created
---For the query to execute successfully, the following tables are required:
                  -- a. spv_rapid_coverage_assessment_form - this is the table that has all the responses from the Rapid Coverage Assessment form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
--- The following section(s) need to updated during customization:
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. province_iso2_codes needs to be updated to match the reporting country office iso2 codes
create or replace view staging.rca as
(
with 
not_vaccinated as 
(
select 
 submission_id,
 sum(indicator_value) filter (where rca_category='Not vaccinated') as total_not_vaccinated
from staging.rca_actuals
group by 1
)
select 
  srcaf.id::bigint as submission_id,
  srcaf.rca_date::date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  srcaf.admin4_lat::real as latitude,
  srcaf.admin4_long::real as longitude,
  a5.label as admin5,
  rl.label as vaccine_administered,
  srcaf.doors_visited::bigint,
  srcaf.rca_mop_up::text,
  rl1.label as conducted_by,
  total_not_vaccinated,
  pic.iso2_code::varchar(50),
  srcaf.submitted_at,
  srcaf.modified_at,
  srcaf.enumerator::text 
from airbyte_removed_group.spv_rapid_coverage_assessment_form srcaf 
left join airbyte_v2.admin1 a1 on srcaf.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join airbyte_v2.admin2 a2 on srcaf.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join airbyte_v2.admin3 a3 on srcaf.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join airbyte_v2.admin4 a4 on srcaf.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join airbyte_v2.admin5 a5 on srcaf.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join airbyte_removed_group.rca_labels rl on rl.code=srcaf.vaccine_administered and rl.question='vaccine_administered' ---adds the vaccine labels from the labels table
left join airbyte_removed_group.rca_labels rl1 on rl1.code=srcaf.conducted_by and rl1.question='conducted_by' ---adds the conducted by labels from the labels table
left join airbyte_v2.province_iso2_codes pic on pic.admin2_id::text=a2.name::text ---adds the admin 2 isocodes to be used for the country maps on superset
left join not_vaccinated nv on srcaf.id=nv.submission_id::varchar
);

----This query creates a view of the RCA sources of information
---For the query to execute successfully, the following tables are required:
                  -- a. spv_rapid_coverage_assessment_form - this is the table that has all the responses from the Rapid Coverage Assessment form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
--- The following section(s) need to updated during customization:
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. The unnest source_info and no_doors section can be updated if the list of sources of information is longer than the one provided
create or replace view staging.rca_sources_info as
(
with sources_info as 
(
select 
  srcaf.id::bigint as submission_id,
  srcaf.rca_date::date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  srcaf.admin4_lat::real as latitude,
  srcaf.admin4_long::real as longitude,
  a5.label as admin5,
  rl.label as vaccine_administered,
  unnest(array['Radio','Tv','Flyers','Other sources of information','Streamer/Tarpaulin','Officials','Social media','Health workers','Relatives/Neighbours']) as sources_info,
  (unnest(array[radio,tv,flyers,"others",streamer,officials,social_media,health_workers::int,relatives_neighbours]))::bigint as no_doors
from airbyte_removed_group.spv_rapid_coverage_assessment_form srcaf 
left join airbyte_v2.admin1 a1 on srcaf.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join airbyte_v2.admin2 a2 on srcaf.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join airbyte_v2.admin3 a3 on srcaf.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join airbyte_v2.admin4 a4 on srcaf.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join airbyte_v2.admin5 a5 on srcaf.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join airbyte_removed_group.rca_labels rl on rl.code=srcaf.vaccine_administered and rl.question='vaccine_administered' ---Adds the vaccine label from the labels column
)
select * from sources_info
where no_doors is not null ---filters out rows with no responses
);



----This query creates a view of the RCA reasons breakdown as to why the children have not been vaccinated 
---For the query to execute successfully, the following tables are required:
                  -- a. spv_rapid_coverage_assessment_form - this is the table that has all the responses from the Rapid Coverage Assessment form on Inform
                  -- b. admin1 - this table contains all the admin1 level admin names and codes
                  -- c. admin2 - this table contains all the admin2 level admin names and codes
                  -- d. admin3 - this table contains all the admin3 level admin names and codes
                  -- e. admin4 - this table contains all the admin4 level admin names and codes
                  -- f. admin5 - this table contains all the admin5 level admin names and codes
--- The following section(s) need to updated during customization:
                  --- a.The admin1, admin2, admin3, admin4 and admin5 files within the CSV schema need to be updated to match the administrative hierarchy of the respective country office adopting the tool
                  --- b. The unnest reasons and reasons_value section can be updated if the list of reasons is longer than the one provided
create or replace view airbyte_removed_group.rca_not_vaccinated_reasons as
(
with reasons_breakdown as 
(
select 
  srcaf.id as submission_id,
  srcaf.rca_date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  srcaf.admin4_lat::real as latitude,
  srcaf.admin4_long::real as longitude,
  a5.label as admin5,
  rl.label as vaccine_administered,
  unnest(array['Reason 1','Reason 2','Reason 3','Reason 4','Reason 5','Reason 6','Reason 7','Reason 8','Reason 9','Reason 10','Other']) as reasons, ---creates a column of the list provided
  unnest(array[reason_1,reason_2,reason_3,reason_4,reason_5,reason_6,reason_7,reason_8,reason_9,reason_10::int,other_reasons]) as reasons_value --- creates a column that has the responses provided under the respective listed fields
from airbyte_removed_group.spv_rapid_coverage_assessment_form_vaccination srcafv 
left join airbyte_removed_group.spv_rapid_coverage_assessment_form srcaf on srcafv._airbyte_ab_id=srcaf._airbyte_ab_id
left join airbyte_v2.admin1 a1 on srcaf.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join airbyte_v2.admin2 a2 on srcaf.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join airbyte_v2.admin3 a3 on srcaf.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join airbyte_v2.admin4 a4 on srcaf.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join airbyte_v2.admin5 a5 on srcaf.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join airbyte_removed_group.rca_labels rl on rl.code=srcaf.vaccine_administered and rl.question='vaccine_administered' ---Adds the vaccine label
)
select * from reasons_breakdown
where reasons_value is not null
);



------POWER BI VIEWS
---The PowerBI connector currently pulls data from views within the public schema. Hence the repointing to the public schema
create or replace view public.rca_actuals as 
(
select * from staging.rca_actuals
);


create or replace view public.rca as 
(
select 
    submission_id,
    rca_date,
    admin1,
    admin2,
    admin3,
    admin4,
    latitude,
    longitude,
    admin5,
    vaccine_administered,
    doors_visited,
    rca_mop_up,
    conducted_by,
    total_not_vaccinated,
    iso2_code,
    submitted_at::date,
    modified_at::date,
    enumerator
from staging.rca
);


create or replace view public.rca_sources_info as 
(
select * from staging.rca_sources_info
);


create or replace view public.rca_not_vaccinated_reasons as 
(
select * from staging.rca_not_vaccinated_reasons
);