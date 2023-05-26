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
create or replace view staging.social_mobilization_indicators as 
(
with rumors_misinformation as 
(
----This section unnests the select multiple option under Rumors and Misinformation
select  
 id,
 unnest(array['Vaccine related','Vaccination team','Vaccination campaign']) as indicators_category,
 unnest(array[vaccine_related,vaccination_team,vaccination_campaign]) as indicators_value
from templates.spv_social_mobilization_indicators ssmi 
),
mobilization_indicators as
(
--This section creates a tidy section of the social mobilization activities such as number of households visited etc.
select 
 id,
 unnest(array['Social mobilisers/community volunteers engaged','Households visited','Group meetings/learning sessions with caregivers conducted',
 'Religious institutions visited','Advocacy meetings with community leaders','Posters and banners produced and displayed']) as indicator_category,
 unnest(array[social_mobilisers,hhs_visited,learning_sessions,religious_institutions,advocacy_meetings,posters_banners]) as indicator_value
from templates.spv_social_mobilization_indicators ssmi 
),
----This section creates a tidy section of the refusals addressed from the spv_social_mobilization_indicators_refusals repeat group data table
refusals_addressed as
(
select 
parent_id as id,
unnest(array['Refusals addressed']) as indicator_category,
unnest(array[refusals_addressed]) as indicator_value
from templates.spv_social_mobilization_indicators_refusals ssmir 
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
  c.id, 
  ssmi.assessment_date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  case 
  	when ssmi.conducted_by='others' then conducted_by_other else conducted_by 
  end as conducted_by,  
  c.indicators_category,
  c.indicators_value,
  ssmir.vaccine_label as vaccine_administered,
  pic.iso2_code,
  ssmi.submitted_at,
  ssmi.modified_at,
  ssmi.enumerator 
from combined_indicators c  
left join templates.spv_social_mobilization_indicators ssmi on c.id=ssmi.id
left join csv.admin1 a1 on ssmi.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on ssmi.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on ssmi.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on ssmi.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join templates.spv_social_mobilization_indicators_refusals ssmir on ssmir.parent_id=c.id
left join csv.province_iso2_codes pic on pic.admin2_id=a2.name
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
