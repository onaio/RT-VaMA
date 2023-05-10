---Creates the templates schema to host the templates from Inform
create schema if not exists templates;
alter schema templates owner to rt_vama;

---Creates the CSV schema to host the location templates and any CSV file that is uploaded to the database a
create schema if not exists csv;
alter schema csv owner to rt_vama;


---Creates the schema that hosts the final views for visualization
create schema if not exists reporting;
alter schema reporting owner to rt_vama;

---Creates a staging schema that hosts the views that have labels from the registry table and views that can be joined to create a final view for visualization
create schema if not exists staging;
alter schema staging owner to rt_vama;

---Supplemental Immunization Activity labels table
--- When pulling data from Inform, the connector separates the labels from the actual values by creating separate tables. Labels are normally defined on the choices worksheet of the XLSForm
--- This view extracts labels from the registry table for the SIA form
create or replace view staging.sia_labels as 
(
-- Extract the json column we want, limit by 1 row since the data has a row for each filled record.
with dd as
(
select 
json -> 'xform:choices' as data 
from templates.registry
where uri = 'Supplemental_Immunization_Activity?t=json&v=202302071328'
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
alter view staging.sia_labels owner to rt_vama;


---Supplemental Immunization Activity
----Template link: https://inform.unicef.org/uniceftemplates/635/759
--- This script unnests the SIA template with actual values
--- The location information has been loaded from external CSV files which have been uploaded to the csv schema
create or replace view staging.sia_actuals as 
(
select 
  siav.parent_id as submission_id,
  sia.date_vaccination_activity,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  sia.admin4_lat::real as latitude,
  sia.admin4_long::real as longitude,
  a5.label as admin5,
  sia.vaccine_label as vaccine_administered,
  siav.age_group_label,
  unnest(array['Vaccinated','Vaccinated','Deferred Vaccinated','Deferred Vaccinated','Refused Vaccinated','Refused Vaccinated','Deferred','Deferred','Refused','Refused']) as coverage_category,
  unnest(array['Males','Females','Males','Females','Males','Females','Males','Females','Males','Females']) as indicator_category,
  unnest(array[vaccinated_males,vaccinated_females,vaccinated_males_previously_deferred,vaccinated_females_previously_deferred,vaccinated_males_previously_refused,vaccinated_females_previously_refused,deferred_males,deferred_females,refused_males,refused_females]) as indicator_value
from templates.supplemental_immunization_activity_vaccine siav 
left join templates.supplemental_immunization_activity sia on siav.parent_id=sia.id  --- Adds the fields assosciated with the repeat group data
left join csv.admin1 a1 on sia.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on sia.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on sia.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on sia.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on sia.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
);
alter view staging.sia_actuals owner to rt_vama;


---This script creates a view that aggregates the actual values to admin 4 level so that the actuals can be matched to the daily target values
create or replace view staging.aggregated_sia_actuals_target as
(
with 
targets as 
--Retrieves the campaign target from the targets form, then computes the daily target based on the number of days the campaign is supposed to take.
--The link to the targets form: https://inform.unicef.org/uniceftemplates/635/977 
(
select 
 siat.admin1,
 siat.admin2,
 siat.admin3,
 siat.admin4,
 siat.admin5,
 siat.campaign_start_date,
 siat.campaign_end_date,
 (siat.campaign_end_date - siat.campaign_start_date) as campaign_days,
 siat.vaccine_label,
 SUM(siattc.no_children) as campaign_target
from templates.supplemental_immunization_activity_target siat  
left join templates.supplemental_immunization_activity_target_target_children siattc on siattc.parent_id=siat.id 
group by 1,2,3,4,5,6,7,8,9
),
actuals as 
---Retrieves the actual values from the vaccination coverage group
(
select 
  sia.date_vaccination_activity,
  sia.admin1,
  sia.admin2,
  sia.admin3,
  sia.admin4,
  sia.admin5,
  sia.vaccine_label as vaccine_administered,
  SUM(siav.vaccinated_males + siav.vaccinated_females) + SUM(siav.vaccinated_males_previously_deferred + siav.vaccinated_females_previously_deferred) + SUM(siav.vaccinated_males_previously_refused + siav.vaccinated_females_previously_refused) as total_vaccinated,
  sia.admin4_lat as latitude,
  sia.admin4_long as longitude
from templates.supplemental_immunization_activity_vaccine siav 
left join templates.supplemental_immunization_activity sia on siav.parent_id=sia.id
group by 1,2,3,4,5,6,7,9,10
),
vaccine_doses as
----Aggregates the vials used, vials discarded and vaccine dose up to admin 4
(
select 
 sia.date_vaccination_activity,
 sia.admin1,
 sia.admin2,
 sia.admin3,
 sia.admin4,
 sia.admin5,
 sia.vaccine_label,
 MAX(sia.vial_dosage) as vial_dosage,
 SUM(sia.vials_used) as vials_used,
 SUM(sia.vial_dosage::int*sia.vials_used) as vaccine_dose,
 SUM(sia.vials_discarded) as vials_discarded 
from templates.supplemental_immunization_activity sia
group by 1,2,3,4,5,6,7
)
select 
 date,
 a1.label as admin1,
 a2.label as admin2,
 a3.label as admin3,
 a4.label as admin4,
 a5.label as admin5,
 a.vaccine_administered,
 a.total_vaccinated as total_vaccinated,
 t.campaign_target as campaign_target,
 vd.vials_used,
 vd.vials_discarded,
 vd.vaccine_dose,
 vd.vial_dosage,
 a.latitude::real,
 a.longitude::real
from csv.hard_coded_dates hcd 
left join actuals a on a.date_vaccination_activity=hcd.date
left join targets t on a.vaccine_administered=t.vaccine_label and t.admin5=a.admin5 and a.date_vaccination_activity between t.campaign_start_date and t.campaign_end_date 
left join csv.admin1 a1 on a.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on a.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on a.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on a.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on a.admin5=a5.name::text ----Adds admin 5 labels using the admin name column
left join vaccine_doses vd on vd.date_vaccination_activity=hcd.date and vd.vaccine_label=a.vaccine_administered and a.admin5=vd.admin5 --matches the value at reporting date, vaccine and admin5 level
where hcd.date<=now()::date and a.date_vaccination_activity is not null ----Filters dates that are not within the actuals form
);
alter view staging.aggregated_sia_actuals_target owner to rt_vama;

create or replace view staging.sia_actuals_target as
(
with 
targets as 
--Retrieves the campaign target from the targets form, then computes the daily target based on the number of days the campaign is supposed to take.
--The link to the targets form: https://inform.unicef.org/uniceftemplates/635/977 
(
select 
 siat.id,
 siat.admin0,
 siat.admin1,
 siat.admin2,
 siat.admin3,
 siat.admin4,
 siat.admin5,
 siat.campaign_start_date,
 siat.campaign_end_date,
 siat.vaccine_label,
 siattc.no_children as campaign_target,
 siattc.no_children / (siat.campaign_end_date - siat.campaign_start_date) as daily_target,
 siattc.age_group_label 
from templates.supplemental_immunization_activity_target siat  
left join templates.supplemental_immunization_activity_target_target_children siattc on siattc.parent_id=siat.id 
),
actuals as 
---Retrieves the actual values from the vaccination coverage group
(
select 
  sia.date_vaccination_activity,
  sia.admin1,
  sia.admin2,
  sia.admin3,
  sia.admin4,
  sia.admin5,
  sia.vaccine_label as vaccine_administered,
  SUM(siav.vaccinated_males + siav.vaccinated_females) as total_vaccinated,
  SUM(siav.deferred_males + siav.deferred_females) as total_deferred,
  SUM(siav.refused_males+ siav.refused_females) as total_refused,
  SUM(siav.vaccinated_males_previously_deferred + siav.vaccinated_females_previously_deferred) as total_vaccinated_previously_deferred,
  SUM(siav.vaccinated_males_previously_refused + siav.vaccinated_females_previously_refused) as total_vaccinated_previously_refused,
  siav.age_group_label,
  sia.admin4_lat as latitude,
  sia.admin4_long as longitude
from templates.supplemental_immunization_activity_vaccine siav 
left join templates.supplemental_immunization_activity sia on siav.parent_id=sia.id
group by 1,2,3,4,5,6,7,13,14,15
)
select 
 date,
 a1.label as admin1,
 a2.label as admin2,
 a3.label as admin3,
 a4.label as admin4,
 a5.label as admin5,
 a.vaccine_administered,
 a.total_vaccinated,
 t.campaign_target,
 a.age_group_label,
 a.latitude::real,
 a.longitude::real,
 pic.iso2_code,
 a.total_deferred,
 a.total_refused,
 a.total_vaccinated_previously_deferred,
 a. total_vaccinated_previously_refused
from csv.hard_coded_dates hcd 
left join actuals a on a.date_vaccination_activity=hcd.date
left join targets t on a.vaccine_administered=t.vaccine_label and t.age_group_label=a.age_group_label and a.admin5=t.admin5 and a.date_vaccination_activity between t.campaign_start_date and t.campaign_end_date  
left join csv.admin1 a1 on a.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on a.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on a.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on a.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on a.admin5=a5.name::text ----Adds admin 5 labels using the admin name column
left join csv.province_iso2_codes pic  on pic.admin2_id::text=a.admin2
where hcd.date<=now()::date and a.date_vaccination_activity is not null ----Filters dates that are not within the actuals form
);
alter view staging.sia_actuals_target owner to rt_vama;



----SIA reasons breakdown
create or replace view staging.deferred_refused_reasons as 
(
with deferred_refused_reasons as 
(
select 
  siav.parent_id as submission_id,
  sia.date_vaccination_activity,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  a5.label as admin5,
  sia.vaccine_label as vaccine_administered,
  siav.age_group_label,
  unnest(array['Reason 1','Reason 2','Reason 3','Reason 4','Reason 5','Reason 6', 'Reason 7','Reason 8','Reason 9', 'Reason 10','Reason 11','Reason 12','Other','Reason 1','Reason 2','Reason 3','Reason 4','Reason 5','Reason 6','Reason 7','Reason 8','Reason 9','Reason 10','Reason 11','Reason 12','Other']) as reason_category,
  unnest(array[deferred_reason_1,deferred_reason_2,deferred_reason_3,deferred_reason_4,deferred_reason_5,deferred_reason_6,deferred_reason_7,deferred_reason_8,deferred_reason_9,deferred_reason_10,deferred_reason_11,deferred_reason_12,deferred_other,refused_reason_1,refused_reason_2,refused_reason_3,refused_reason_4,refused_reason_5,refused_reason_6,refused_reason_7,refused_reason_8,refused_reason_9,refused_reason_10,refused_reason_11,refused_reason_12,refused_other]) as reasons_value,
  unnest(array['Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Deferred','Refused','Refused','Refused','Refused','Refused','Refused','Refused','Refused','Refused','Refused','Refused','Refused','Refused']) as coverage_category
from templates.supplemental_immunization_activity_vaccine siav
left join templates.supplemental_immunization_activity sia on siav.parent_id=sia.id  --- Adds the fields assosciated with the repeat group data
left join csv.admin1 a1 on sia.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on sia.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on sia.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on sia.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on sia.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
)
select * from deferred_refused_reasons
where reasons_value is not null
);

alter view staging.deferred_refused_reasons owner to rt_vama;


----SIA Records
---This query creates a view that can be used to show the no. of records/double counting
create or replace view staging.sia_records as 
(
select 
  sia.id as submission_id,
  sia.date_vaccination_activity,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  a5.label as admin5,
  sia.vaccine_label as vaccine_administered,
  sia.submitted_at,
  sia.modified_at,
  sia.enumerator 
from templates.supplemental_immunization_activity sia 
left join csv.admin1 a1 on sia.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on sia.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on sia.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on sia.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on sia.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
);
alter view staging.sia_records owner to rt_vama;

---This view creates the social mobilization indicators
--- The link to the template: https://inform.unicef.org/uniceftemplates/635/765
--- The social mobilization indicators data entails communication activities during or after a campaign.
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
 unnest(array['Social mobilisers/community volunteers engaged','Households visited','Group meetings/learning sessions with caregivers conducted','Religious institutions visited','Advocacy meetings with community leaders','Posters and banners produced and displayed']) as indicator_category,
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

alter view staging.social_mobilization_indicators owner to rt_vama;

 -----Synchronized Vaccination Monitoring Tool
----Inform link: https://inform.unicef.org/uniceftemplates/635/847
---This view creates a tidy table of the Synchronized Vaccination Monitoring Tool form
create or replace view staging.monitoring_tool as
(
with microplan_vaccine_management as
(
select 
  svmt.id,
  unnest(array[part1_indicator1,part1_indicator2,part1_indicator3,part1_indicator4,part1_indicator5,part1_indicator6,part1_indicator7,part1_indicator8,part1_indicator9,part1_indicator10,part1_indicator11,part1_indicator12,part1_indicator13,part1_indicator14,part1_indicator15,part1_indicator16,part1_indicator17,part1_indicator18,part1_indicator19,part1_indicator20,part1_indicator21,part1_indicator22,part1_indicator23,part1_indicator24,part2_indicator1,part2_indicator2,part2_indicator3,part2_indicator4,part2_indicator5,part2_indicator6,part2_indicator7,part2_indicator8,part2_indicator9,part2_indicator10,part2_indicator11,part2_indicator12,part2_indicator13,part2_indicator14,part2_indicator15,part2_indicator16,part2_indicator17,part2_indicator18,part2_indicator19,part2_indicator20,part2_indicator21,part2_indicator22,part2_indicator23,part2_indicator24,part2_indicator25,part2_indicator26,part2_indicator27,part2_indicator28,part2_indicator29,part2_indicator30,part2_indicator28a::text,part2_indicator29a::text,part2_indicator30a::text]) as indicators_value,
  unnest(array[part1_indicator1_remarks,part1_indicator2_remarks,part1_indicator3_remarks,part1_indicator4_remarks,part1_indicator5_remarks,part1_indicator6_remarks,part1_indicator7_remarks,part1_indicator8_remarks,part1_indicator9_remarks,part1_indicator10_remarks,part1_indicator11_remarks,part1_indicator12_remarks,part1_indicator13_remarks,part1_indicator14_remarks,part1_indicator15_remarks,part1_indicator16_remarks,part1_indicator17_remarks,part1_indicator18_remarks,part1_indicator19_remarks,part1_indicator20_remarks,part1_indicator21_remarks,part1_indicator22_remarks,part1_indicator23_remarks,part1_indicator24_remarks,part2_indicator1_remarks,part2_indicator2_remarks,part2_indicator3_remarks,part2_indicator4_remarks,part2_indicator5_remarks,part2_indicator6_remarks,part2_indicator7_remarks,part2_indicator8_remarks,part2_indicator9_remarks,part2_indicator10_remarks,part2_indicator11_remarks,part2_indicator12_remarks,part2_indicator13_remarks,part2_indicator14_remarks,part2_indicator15_remarks,part2_indicator16_remarks,part2_indicator17_remarks,part2_indicator18_remarks,part2_indicator19_remarks,part2_indicator20_remarks,part2_indicator21_remarks,part2_indicator22_remarks,part2_indicator23_remarks,part2_indicator24_remarks,part2_indicator25_remarks,part2_indicator26_remarks,part2_indicator27_remarks,part2_indicator28_remarks,part2_indicator29_remarks,part2_indicator30_remarks,part2_indicator28b,part2_indicator29b,part2_indicator30b]) as indicators_remarks,
  unnest(array['Presence of data board','Presence of health center microplan','Presence of spot map','Indication of population/specific target','Inclusion of activities for social preparation','Inclusion of dialogues with local officials/CSG','Public announcements are made','Evidence that social mobilization were done','Presence of activities to enable access in hard to reach areas are expected','Training of vaccination teams on comms and social mobilization','Presence of daily itinerary schedule','Presence of specific vaccination strategy','Supervisory plan','Presence of separate sheet for vaccines and other logistic calculations','Enough campaign forms','Enough mother/child book or vaccination cards','Presence of transportation support','Response/referral for AEFI','Presence of contingency plan to include emergencies in case of absence of vaccination team member','Schedule for mop ups','Plan for RCA intra-campaign','Evidence of regular feedback meeting','Health care waste plan','Follow up visits','Presence of health facility management plan','Presence of continuous electricity supply','Presence of generator/solar power that can be used in case of intermittent power supply','Presence of refrigiration that can be used for vaccine','Vaccines placed in separate box','Proper label is used for vaccine','Vaccines are stored with appropriate temperature','Presence of adequate temperature monitoring devices','Conduct of regular temperature monitoring','Proper temperature monitoring','Note of temperature breach','Availability of ice pack freezing capacity','Recording of vaccines that are issued daily','Proper filling up of forms','Presence of enough vaccine carriers','Presence of enough ice packs','Providing immunzation at a fixed post','Presence of vaccine carrier that is separately label','Use of resealable plastic','Use of resealable plastic for used vials','Return of reusable vials','Accounting of all collected vials','Presence of vaccine accountability monitor','Placing of collected vials in a secured container','Empty vials, sealed properly','Returning of un-opened/un used vial','Account of used and unused vials','Missing vials identified','Replaced vials identified','Damaged vials','Number of missing vials', 'Number of replaced vials','Number of damaged vials']) as indicators_label,
  unnest(array['Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management'])as indicators_category
from templates.synchronized_vaccination_monitoring_tool svmt
),
vaccination_site as
(
select 
  svmtvti.parent_id,
  unnest(array[part3_indicator1,part3_indicator2,part3_indicator3,part3_indicator4,part3_indicator5,part3_indicator6,part3_indicator7,part3_indicator8,part3_indicator9,part3_indicator10,part3_indicator11,part3_indicator12,part3_indicator13,part3_indicator14,part3_indicator15,part3_indicator16,part3_indicator17,part3_indicator18])as indicators_value,
  unnest(array[part3_indicator1_remarks,part3_indicator2_remarks,part3_indicator3_remarks,part3_indicator4_remarks,part3_indicator5_remarks,part3_indicator6_remarks,part3_indicator7_remarks,part3_indicator8_remarks,part3_indicator9_remarks,part3_indicator10_remarks,part3_indicator11_remarks,part3_indicator12_remarks,part3_indicator13_remarks,part3_indicator14_remarks,part3_indicator15_remarks,part3_indicator16_remarks,part3_indicator17_remarks,part3_indicator18_remarks])as indicators_remarks,
  unnest(array['Supervisor visits and supervises team','Microplan followed and reviewed','Vaccination strategies followed','Vaccine carrier carried','Use of resealable plastic','Vaccines are stored with appropriate temperatures','Encountered difficulties at site','Vaccines properly recorded','Finger markings correctly applied','Doses and vaccinated children properly recorded','Doors properly marked','Attaining of daily targets tracked','Healthcare waste appropriately handled','Vaccination team wearing PPE','Hand hygiene practised','Caregivers reminded to follow up routine vaccination','Caregivers reminded what to do in case of reactions','Members asking questions related to AFP'])as indicators_label,
  unnest(array['At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site'])as indicators_category
from templates.synchronized_vaccination_monitoring_tool_vaccination_team_info svmtvti    
),
monitoring_tool as 
(
select id, indicators_value, indicators_remarks, indicators_label, indicators_category from microplan_vaccine_management
union all
select parent_id,indicators_value, indicators_remarks, indicators_label, indicators_category from vaccination_site
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
where mt.indicators_value is not null
);
alter view staging.monitoring_tool owner to rt_vama;

-----Synchronized Vaccination Monitoring Tool
---This view categorizes monitored facilities
create or replace view staging.monitored_facilities_overall_proportion as 
(
with microplan_vaccine_management as
(
select 
  svmt.id,
  unnest(array[part1_indicator1,part1_indicator2,part1_indicator3,part1_indicator4,part1_indicator5,part1_indicator6,part1_indicator7,part1_indicator8,part1_indicator9,part1_indicator10,part1_indicator11,part1_indicator12,part1_indicator13,part1_indicator14,part1_indicator15,part1_indicator16,part1_indicator17,part1_indicator18,part1_indicator19,part1_indicator20,part1_indicator21,part1_indicator22,part1_indicator23,part1_indicator24,part2_indicator1,part2_indicator2,part2_indicator3,part2_indicator4,part2_indicator5,part2_indicator6,part2_indicator7,part2_indicator8,part2_indicator9,part2_indicator10,part2_indicator11,part2_indicator12,part2_indicator13,part2_indicator14,part2_indicator15,part2_indicator16,part2_indicator17,part2_indicator18,part2_indicator19,part2_indicator20,part2_indicator21,part2_indicator22,part2_indicator23,part2_indicator24,part2_indicator25,part2_indicator26,part2_indicator27,part2_indicator28,part2_indicator29,part2_indicator30]) as indicators_value,
  unnest(array[part1_indicator1_remarks,part1_indicator2_remarks,part1_indicator3_remarks,part1_indicator4_remarks,part1_indicator5_remarks,part1_indicator6_remarks,part1_indicator7_remarks,part1_indicator8_remarks,part1_indicator9_remarks,part1_indicator10_remarks,part1_indicator11_remarks,part1_indicator12_remarks,part1_indicator13_remarks,part1_indicator14_remarks,part1_indicator15_remarks,part1_indicator16_remarks,part1_indicator17_remarks,part1_indicator18_remarks,part1_indicator19_remarks,part1_indicator20_remarks,part1_indicator21_remarks,part1_indicator22_remarks,part1_indicator23_remarks,part1_indicator24_remarks,part2_indicator1_remarks,part2_indicator2_remarks,part2_indicator3_remarks,part2_indicator4_remarks,part2_indicator5_remarks,part2_indicator6_remarks,part2_indicator7_remarks,part2_indicator8_remarks,part2_indicator9_remarks,part2_indicator10_remarks,part2_indicator11_remarks,part2_indicator12_remarks,part2_indicator13_remarks,part2_indicator14_remarks,part2_indicator15_remarks,part2_indicator16_remarks,part2_indicator17_remarks,part2_indicator18_remarks,part2_indicator19_remarks,part2_indicator20_remarks,part2_indicator21_remarks,part2_indicator22_remarks,part2_indicator23_remarks,part2_indicator24_remarks,part2_indicator25_remarks,part2_indicator26_remarks,part2_indicator27_remarks,part2_indicator28_remarks,part2_indicator29_remarks,part2_indicator30_remarks]) as indicators_remarks,
  unnest(array['Presence of data board','Presence of health center microplan','Presence of spot map','Indication of population/specific target','Inclusion of activities for social preparation','Inclusion of dialogues with local officials/CSG','Public announcements are made','Evidence that social mobilization were done','Presence of activities to enable access in hard to reach areas are expected','Training of vaccination teams on comms and social mobilization','Presence of daily itinerary schedule','Presence of specific vaccination strategy','Supervisory plan','Presence of separate sheet for vaccines and other logistic calculations','Enough campaign forms','Enough mother/child book or vaccination cards','Presence of transportation support','Response/referral for AEFI','Presence of contingency plan to include emergencies in case of absence of vaccination team member','Schedule for mop ups','Plan for RCA intra-campaign','Evidence of regular feedback meeting','Health care waste plan','Follow up visits','Presence of health facility management plan','Presence of continuous electricity supply','Presence of generator/solar power that can be used in case of intermittent power supply','Presence of refrigiration that can be used for vaccine','Vaccines placed in separate box','Proper label is used for vaccine','Vaccines are stored with appropriate temperature','Presence of adequate temperature monitoring devices','Conduct of regular temperature monitoring','Proper temperature monitoring','Note of temperature breach','Availability of ice pack freezing capacity','Recording of vaccines that are issued daily','Proper filling up of forms','Presence of enough vaccine carriers','Presence of enough ice packs','Providing immunzation at a fixed post','Presence of vaccine carrier that is separately label','Use of resealable plastic','Use of resealable plastic for used vials','Return of reusable vials','Accounting of all collected vials','Presence of vaccine accountability monitor','Placing of collected vials in a secured container','Empty vials, sealed properly','Returning of un-opened/un used vial','Account of used and unused vials','Missing vials identified','Replaced vials identified','Damaged vials']) as indicators_label,
  unnest(array['Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Information board and microplan check','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management','Vaccine management'])as indicators_category
from templates.synchronized_vaccination_monitoring_tool svmt
),
vaccination_site as
(
select 
  svmtvti.parent_id,
  unnest(array[part3_indicator1,part3_indicator2,part3_indicator3,part3_indicator4,part3_indicator5,part3_indicator6,part3_indicator7,part3_indicator8,part3_indicator9,part3_indicator10,part3_indicator11,part3_indicator12,part3_indicator13,part3_indicator14,part3_indicator15,part3_indicator16,part3_indicator17,part3_indicator18])as indicators_value,
  unnest(array[part3_indicator1_remarks,part3_indicator2_remarks,part3_indicator3_remarks,part3_indicator4_remarks,part3_indicator5_remarks,part3_indicator6_remarks,part3_indicator7_remarks,part3_indicator8_remarks,part3_indicator9_remarks,part3_indicator10_remarks,part3_indicator11_remarks,part3_indicator12_remarks,part3_indicator13_remarks,part3_indicator14_remarks,part3_indicator15_remarks,part3_indicator16_remarks,part3_indicator17_remarks,part3_indicator18_remarks])as indicators_remarks,
  unnest(array['Supervisor visits and supervises team','Microplan followed and reviewed','Vaccination strategies followed','Vaccine carrier carried','Use of resealable plastic','Vaccines are stored with appropriate temperatures','Encountered difficulties at site','Vaccines properly recorded','Finger markings correctly applied','Doses and vaccinated children properly recorded','Doors properly marked','Attaining of daily targets tracked','Healthcare waste appropriately handled','Vaccination team wearing PPE','Hand hygiene practised','Caregivers reminded to follow up routine vaccination','Caregivers reminded what to do in case of reactions','Members asking questions related to AFP'])as indicators_label,
  unnest(array['At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site','At Vaccination site'])as indicators_category
from templates.synchronized_vaccination_monitoring_tool_vaccination_team_info svmtvti    
),
monitoring_tool as 
(
select id, indicators_value, indicators_remarks, indicators_label, indicators_category from microplan_vaccine_management
union all
select parent_id,indicators_value, indicators_remarks, indicators_label, indicators_category from vaccination_site
),
yes_responded_questions as
(
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
  svmt.vaccine_label, 
  mt.indicators_value,
  mt.indicators_remarks, 
  mt.indicators_label,
  mt.indicators_category,
  smt.no_of_questions,
  count(mt.id) filter (where mt.indicators_value='Yes') as count_yes_questions
from monitoring_tool mt
left join csv.synchronized_monitoring_tool smt on mt.indicators_category=smt.indicators_category ---adds no.of questions in the category
left join templates.synchronized_vaccination_monitoring_tool svmt on mt.id=svmt.id ---adds the missing fields in the repeat group
left join csv.admin1 a1 on svmt.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on svmt.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on svmt.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on svmt.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on svmt.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
where mt.indicators_value is not null
group by 1,svmt.date_vaccination_activity,a1.label,a2.label,a3.label,a4.label,svmt.admin4_lat,svmt.admin4_long,a5.label,svmt.vaccine_label,mt.indicators_value,mt.indicators_remarks,mt.indicators_label,mt.indicators_category,smt.no_of_questions
),
yes_category_proportion as 
(
select 
id,
date_vaccination_activity,
admin1,
admin2,
admin3,
admin4,
admin5,
latitude,
longitude,
vaccine_label,
indicators_category,
(sum(count_yes_questions)/max(no_of_questions))*100 as yes_category_proportion 
from yes_responded_questions yrq
group by 1,2,3,4,5,6,7,8,9,10,11
)
select 
id,
date_vaccination_activity,
admin1,
admin2,
admin3,
admin4,
admin5,
latitude,
longitude,
vaccine_label as vaccine_administered,
(sum(case when yes_category_proportion=100 then 1 else 0 end)::float/ 3)*100 as overall_facility_proportion,
case 
	when ((sum(case when yes_category_proportion=100 then 1 else 0 end)::float/ 3)*100) >=0 and ((sum(case when yes_category_proportion=100 then 1 else 0 end)::float/ 3)*100) < 50 then '<50%'
	when ((sum(case when yes_category_proportion=100 then 1 else 0 end)::float/ 3)*100) >=50 and ((sum(case when yes_category_proportion=100 then 1 else 0 end)::float/ 3)*100) <= 99 then '50-99%'
	when ((sum(case when yes_category_proportion=100 then 1 else 0 end)::float/ 3)*100) =100 then '100%'
end as overall_proportion_category
from yes_category_proportion
group by 1,2,3,4,5,6,7,8,9,10
);

alter view staging.monitored_facilities_overall_proportion owner to rt_vama;


----Vaccinated children in the monitoring tool
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
left join csv.province_iso2_codes pic on pic.admin2_id::text=a2.name::text
);

alter view staging.vaccinated_children_under_monitoring_tool owner to rt_vama;

---- Health Center Level Monitoring and Assessment of Readiness
---- Inform link: https://inform.unicef.org/uniceftemplates/635/761
---This view creates a tidy table of the Health Center Level Monitoring and Assessment of Readiness form
create or replace view staging.hcl_monitoring_assessment as 
with hc_assessment as 
(
select 
hcl.id,
hcl.assessment_date,
hcl.vaccine_label,
a1.label as admin1,
a2.label as admin2,
a3.label as admin3,
a4.label as admin4,
hcl.admin4_lat::real as latitude,
hcl.admin4_long::real as longitude,
a5.label as admin5,
unnest(array[microplan_indicator1,microplan_indicator2,microplan_indicator3,microplan_indicator4,microplan_indicator5,microplan_indicator6,microplan_indicator7,microplan_indicator8,microplan_indicator9,logistics_indicator1,logistics_indicator2,logistics_indicator3,logistics_indicator4,logistics_indicator5,logistics_indicator6,logistics_indicator7,social_mob_indicator1,social_mob_indicator2,social_mob_indicator3,imm_safety_indicator1,imm_safety_indicator2,supervision_indicator1,supervision_indicator2,supervision_indicator3,supervision_indicator4,supervision_indicator5,reporting_indicator1,reporting_indicator2,reporting_indicator3,vacc_mngt_indicator1,vacc_mngt_indicator2,vacc_mngt_indicator3,vacc_mngt_indicator4,hr_indicator1]) as indicators_value,
unnest(array[microplan_indicator1_remarks,microplan_indicator2_remarks,microplan_indicator3_remarks,microplan_indicator4_remarks,microplan_indicator5_remarks,microplan_indicator6_remarks,microplan_indicator7_remarks,microplan_indicator8_remarks,microplan_indicator9_remarks,logistics_indicator1_remarks,logistics_indicator2_remarks,logistics_indicator3_remarks,logistics_indicator4_remarks,logistics_indicator5_remarks,logistics_indicator6_remarks,logistics_indicator7_remarks,social_mob_indicator1_remarks,social_mob_indicator2_remarks,social_mob_indicator3_remarks,imm_safety_indicator1_remarks,imm_safety_indicator2_remarks,supervision_indicator1_remarks ,supervision_indicator2_remarks,supervision_indicator3_remarks,supervision_indicator4_remarks,supervision_indicator5_remarks,reporting_indicator1_remarks,reporting_indicator2_remarks,reporting_indicator3_remarks,vacc_mngt_indicator1_remarks,vacc_mngt_indicator2_remarks,vacc_mngt_indicator3_remarks,vacc_mngt_indicator4_remarks,hr_indicator2_remarks]) as indicators_remarks,
unnest(array['Inclusion of all areas in the health center microplan','List of all transit and congregation points,markets and religious gathering available','A plan to reach high-risk populations included','Daily activity plans for the teams are available','Special strategies clearly planned','Maps show catchment areas','Logistics and other resource estimations are complete','List of local influencers and contact details available','Vaccine and waste management plan in place','Cold chain capacity and contigency plans for vaccine storage available','Availability of adequate quantity of vaccines','Availability of adequate quantity of vaccination essentials(eg. vaccine carries,ice packs)',' Face mask and hand hygiene available','Other logistics received (finger markers etc)','Logistics transport available to supply all areas','Contigency plan in place for replenishment when stocks run low','Engagement of leaders/officials for campaign announcements and meetings confirmed','Display of promotion materials in conspicuous places','Community aware about the assigned date and venue of vaccination sessions','Supervisors know how to report AEFI and communicate risk in case of AEFIs','AEFI investigation forms and SOPs available with supervisors','Monitoring and supervision plan available','Supervisors trained for conducting team monitoring and RCMs','Required checklists and templates available','Mop-up system in place in areas with un-immunized/missed children after RCA','Daily monitoring of coverage data and feedback system available','Daily collection and consolidation of tally sheets system available','Mechanism in place for submission of reports','ODK orientation conducted','Logistics focal point assigned and trained on vaccine management','Separate space allocation for vaccine and clear labelling in refrigerator','Required recording and reporting templates available','System in place for vaccine recall,accuntability,collection,handover and reporting','Adequate number of vaccinators and recorders']) as indicators_label,
unnest(array['Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Logistics Supply','Logistics Supply','Logistics Supply','Logistics Supply','Logistics Supply','Logistics Supply','Logistics Supply','Social Mobilization','Social Mobilization','Social Mobilization','Immunization Safety','Immunization Safety','Supervision and Monitoring','Supervision and Monitoring','Supervision and Monitoring','Supervision and Monitoring','Supervision and Monitoring','Reporting System','Reporting System','Reporting System','Vaccine Management','Vaccine Management','Vaccine Management','Vaccine Management','Human Resource']) as indicators_category
from templates.health_center_level_monitoring_and_assessment_of_readiness hcl 
left join csv.admin1 a1 on hcl.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on hcl.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on hcl.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on hcl.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on hcl.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
)
select
ha.id,
max(ha.assessment_date) as assessment_date,
ha.vaccine_label,
ha.admin1,
ha.admin2,
ha.admin3,
ha.admin4,
ha.latitude,
ha.longitude,
ha.admin5,
ha.indicators_value,
ha.indicators_remarks,
ha.indicators_label,
ha.indicators_category,
haqpc.no_of_questions,
hcl.submitted_at,
hcl.modified_at,
hcl.enumerator 
from hc_assessment ha ----This file has the list of indicator categories with no.of questions per category
left join csv.hc_assessment_questions_per_category haqpc on ha.indicators_category=haqpc.indicators_category 
left join templates.health_center_level_monitoring_and_assessment_of_readiness hcl on ha.id=hcl.id 
group by 1,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
;
alter view staging.hcl_monitoring_assessment owner to rt_vama;

----This query creates a view that calculates the facilities which are ready for the campaign
create or replace view staging.ready_facilities as 
with hc_assessment as 
(
select 
hcl.id,
hcl.assessment_date,
hcl.vaccine_label,
a1.label as admin1,
a2.label as admin2,
a3.label as admin3,
a4.label as admin4,
hcl.admin4_lat::real as latitude,
hcl.admin4_long::real as longitude,
a5.label as admin5,
unnest(array[microplan_indicator1,microplan_indicator2,microplan_indicator3,microplan_indicator4,microplan_indicator5,microplan_indicator6,microplan_indicator7,microplan_indicator8,microplan_indicator9,logistics_indicator1,logistics_indicator2,logistics_indicator3,logistics_indicator4,logistics_indicator5,logistics_indicator6,logistics_indicator7,social_mob_indicator1,social_mob_indicator2,social_mob_indicator3,imm_safety_indicator1,imm_safety_indicator2,supervision_indicator1,supervision_indicator2,supervision_indicator3,supervision_indicator4,supervision_indicator5,reporting_indicator1,reporting_indicator2,reporting_indicator3,vacc_mngt_indicator1,vacc_mngt_indicator2,vacc_mngt_indicator3,vacc_mngt_indicator4,hr_indicator1]) as indicators_value,
unnest(array[microplan_indicator1_remarks,microplan_indicator2_remarks,microplan_indicator3_remarks,microplan_indicator4_remarks,microplan_indicator5_remarks,microplan_indicator6_remarks,microplan_indicator7_remarks,microplan_indicator8_remarks,microplan_indicator9_remarks,logistics_indicator1_remarks,logistics_indicator2_remarks,logistics_indicator3_remarks,logistics_indicator4_remarks,logistics_indicator5_remarks,logistics_indicator6_remarks,logistics_indicator7_remarks,social_mob_indicator1_remarks,social_mob_indicator2_remarks,social_mob_indicator3_remarks,imm_safety_indicator1_remarks,imm_safety_indicator2_remarks,supervision_indicator1_remarks ,supervision_indicator2_remarks,supervision_indicator3_remarks,supervision_indicator4_remarks,supervision_indicator5_remarks,reporting_indicator1_remarks,reporting_indicator2_remarks,reporting_indicator3_remarks,vacc_mngt_indicator1_remarks,vacc_mngt_indicator2_remarks,vacc_mngt_indicator3_remarks,vacc_mngt_indicator4_remarks,hr_indicator2_remarks]) as indicators_remarks,
unnest(array['Inclusion of all areas in the health center microplan','List of all transit and congregation points,markets and religious gathering available','A plan to reach high-risk populations included','Daily activity plans for the teams are available','Special strategies clearly planned','Maps show catchment areas','Logistics and other resource estimations are complete','List of local influencers and contact details available','Vaccine and waste management plan in place','Cold chain capacity and contigency plans for vaccine storage available','Availability of adequate quantity of vaccines','Availability of adequate quantity of vaccination essentials(eg. vaccine carries,ice packs)',' Face mask and hand hygiene available','Other logistics received (finger markers etc)','Logistics transport available to supply all areas','Contigency plan in place for replenishment when stocks run low','Engagement of leaders/officials for campaign announcements and meetings confirmed','Display of promotion materials in conspicuous places','Community aware about the assigned date and venue of vaccination sessions','Supervisors know how to report AEFI and communicate risk in case of AEFIs','AEFI investigation forms and SOPs available with supervisors','Monitoring and supervision plan available','Supervisors trained for conducting team monitoring and RCMs','Required checklists and templates available','Mop-up system in place in areas with un-immunized/missed children after RCA','Daily monitoring of coverage data and feedback system available','Daily collection and consolidation of tally sheets system available','Mechanism in place for submission of reports','ODK orientation conducted','Logistics focal point assigned and trained on vaccine management','Separate space allocation for vaccine and clear labelling in refrigerator','Required recording and reporting templates available','System in place for vaccine recall,accuntability,collection,handover and reporting','Adequate number of vaccinators and recorders']) as indicators_label,
unnest(array['Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Microplanning','Logistics Supply','Logistics Supply','Logistics Supply','Logistics Supply','Logistics Supply','Logistics Supply','Logistics Supply','Social Mobilization','Social Mobilization','Social Mobilization','Immunization Safety','Immunization Safety','Supervision and Monitoring','Supervision and Monitoring','Supervision and Monitoring','Supervision and Monitoring','Supervision and Monitoring','Reporting System','Reporting System','Reporting System','Vaccine Management','Vaccine Management','Vaccine Management','Vaccine Management','Human Resource']) as indicators_category
from templates.health_center_level_monitoring_and_assessment_of_readiness hcl 
left join csv.admin1 a1 on hcl.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on hcl.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on hcl.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on hcl.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on hcl.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
),
----Gets the number of facilities that have 'Yes' as a response to the questions
value as 
(
select 
admin1,
admin2,
admin3,
admin4,
admin5,
latitude,
longitude,
ha.indicators_category,
count(id) filter (where indicators_value='Yes') as count_
,haqpc.no_of_questions 
from hc_assessment ha ----This file has the list of indicator categories with no.of questions per category
left join csv.hc_assessment_questions_per_category haqpc on ha.indicators_category=haqpc.indicators_category 
group by 1,2,3,4,5,6,7,8,id,indicators_value,no_of_questions
),
-----Calculates the proportion of yes responses based on the no.of questions per category
prop as  
(
select 
admin1,
admin2,
admin3,
admin4,
admin5,
latitude,
longitude,
indicators_category,
sum(count_)/max(no_of_questions) as yes_category_proportion
from value
group by 1,2,3,4,5,6,7,8
)
----Assigns 'Yes' to all the facilities that have a sum of 8 which is the number of categories available.
select 
admin1,
admin2,
admin3,
admin4,
admin5,
latitude,
longitude,
sum(yes_category_proportion) as readiness_value,
case when sum(yes_category_proportion)=8 then 'Yes' else 'No' end as facility_ready,
round(((SUM(
case when yes_category_proportion=1 then 1 else 0 end)::float / 8)*100)::int,0) as category_value,
case   
	when (round(((SUM(case when yes_category_proportion=1 then 1 else 0 end)::float / 8)*100)::int,0)) >=0 and (round(((SUM(case when yes_category_proportion=1 then 1 else 0 end)::float / 8)*100)::int,0))<50 then '<50%'
	when (round(((SUM(case when yes_category_proportion=1 then 1 else 0 end)::float / 8)*100)::int,0)) >=50 and (round(((SUM(case when yes_category_proportion=1 then 1 else 0 end)::float / 8)*100)::int,0))<=99 then '50-99%'
	when (round(((SUM(case when yes_category_proportion=1 then 1 else 0 end)::float / 8)*100)::int,0)) =100 then '100%' else null
end as category_label
from prop
group by 1,2,3,4,5,6,7;

alter view staging.ready_facilities owner to rt_vama;



-----Rapid Convenience Assessment
--The link to the template:https://inform.unicef.org/uniceftemplates/635/762

---Labels view
--- When pulling data from Inform, the connector separates the labels from the actual values by creating separate tables. Labels are normally defined on the choices worksheet of the XLSForm
--- This view extracts labels from the registry table for the RCA form
create or replace view staging.rca_labels as 
(
-- Extract the json column we want, limit by 1 row since the data has a row for each filled record.
with dd as
(
select 
json -> 'xform:choices' as data 
from templates.registry
where uri = 'spv_rapid_coverage_assessment_form?t=json&v=202301251254'
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
alter view staging.rca_labels owner to rt_vama;

----Tidy table
--This query creates a tidy table of the RCA form actual values ie children present in the hh, vaccinated children and not vaccinated children
create or replace view staging.rca_actuals as
(
select 
  srcafv.parent_id as submission_id,
  srcaf.rca_date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  bg.latitude,
  bg.longitude,
  a5.label as admin5,
  rl.label as vaccine_administered,
  srcafv.age_group_label,
  unnest(array['Present in hh','Present in hh','Vaccinated','Vaccinated','Not vaccinated','Not vaccinated']) as rca_category,
  unnest(array['Males','Females','Males','Females','Males','Females']) as indicator_category,
  unnest(array[present_males,present_females,vaccinated_males,vaccinated_females,not_vaccinated_males,not_vaccinated_females]) as indicator_value,
  pic.iso2_code
from templates.spv_rapid_coverage_assessment_form_vaccination srcafv 
left join templates.spv_rapid_coverage_assessment_form srcaf  on srcafv.parent_id=srcaf.id  --- Adds the fields assosciated with the repeat group data
left join csv.admin1 a1 on srcaf.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on srcaf.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on srcaf.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on srcaf.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on srcaf.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join csv.barangay_gps bg on srcaf.admin4=bg.barangay_code::text
left join staging.rca_labels rl on rl.code=srcaf.vaccine_administered and rl.question='vaccine_administered'
left join csv.province_iso2_codes pic on pic.admin2_id::text=a2.name::text
);

alter view staging.rca_actuals owner to rt_vama;


----This query creates a view that has the no.of doors visited, whether mop-up is needed and number of children not vaccinated in the doors visited
create or replace view staging.rca as
(
select 
  srcaf.id as submission_id,
  srcaf.rca_date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  bg.latitude,
  bg.longitude,
  a5.label as admin5,
  rl.label as vaccine_administered,
  srcaf.doors_visited,
  rca_mop_up,
  rl1.label as conducted_by,
  sum(ra.indicator_value) as total_not_vaccinated,
  pic.iso2_code,
  srcaf.submitted_at,
  srcaf.modified_at,
  srcaf.enumerator 
from templates.spv_rapid_coverage_assessment_form srcaf 
left join csv.admin1 a1 on srcaf.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on srcaf.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on srcaf.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on srcaf.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on srcaf.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join csv.barangay_gps bg on srcaf.admin4=bg.barangay_code::text
left join staging.rca_labels rl on rl.code=srcaf.vaccine_administered and rl.question='vaccine_administered'
left join staging.rca_labels rl1 on rl1.code=srcaf.conducted_by and rl1.question='conducted_by'
left join staging.rca_actuals ra on srcaf.rca_date=ra.rca_date and rl.label=ra.vaccine_administered and a5.label=ra.admin5 and ra.rca_category='Not vaccinated'
left join csv.province_iso2_codes pic on pic.admin2_id::text=a2.name::text
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,15
);
alter view staging.rca owner to rt_vama;

----This query creates a view of the RCA sources of information
create or replace view staging.rca_sources_info as
(
with sources_info as 
(
select 
  srcaf.id as submission_id,
  srcaf.rca_date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  bg.latitude,
  bg.longitude,
  a5.label as admin5,
  rl.label as vaccine_administered,
  unnest(array['Radio','Tv','Flyers','Other sources of information','Streamer/Tarpaulin','Officials','Social media','Health workers','Relatives/Neighbours']) as sources_info,
  unnest(array[radio,tv,flyers,"others",streamer,officials,social_media,health_workers,relatives_neighbours]) as no_doors
from templates.spv_rapid_coverage_assessment_form srcaf 
left join csv.admin1 a1 on srcaf.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on srcaf.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on srcaf.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on srcaf.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on srcaf.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join csv.barangay_gps bg on srcaf.admin4=bg.barangay_code::text
left join staging.rca_labels rl on rl.code=srcaf.vaccine_administered and rl.question='vaccine_administered'
)
select * from sources_info
where no_doors is not null
);

alter view staging.rca_sources_info owner to rt_vama;

----RCA reasons not vaccinated breakdown
create or replace view staging.rca_not_vaccinated_reasons as
(
with reasons_breakdown as 
(
select 
  srcafv.parent_id as submission_id,
  srcaf.rca_date,
  a1.label as admin1,
  a2.label as admin2,
  a3.label as admin3,
  a4.label as admin4,
  bg.latitude,
  bg.longitude,
  a5.label as admin5,
  rl.label as vaccine_administered,
  unnest(array['Reason 1','Reason 2','Reason 3','Reason 4','Reason 5','Reason 6','Reason 7','Reason 8','Reason 9','Reason 10','Other']) as reasons,
  unnest(array[reason_1,reason_2,reason_3,reason_4,reason_5,reason_6,reason_7,reason_8,reason_9,reason_10,other_reasons]) as reasons_value
from templates.spv_rapid_coverage_assessment_form_vaccination srcafv 
left join templates.spv_rapid_coverage_assessment_form srcaf on srcaf.id=srcafv.parent_id
left join csv.admin1 a1 on srcaf.admin1=a1.name::text ---Adds admin 1 labels using the admin name column
left join csv.admin2 a2 on srcaf.admin2=a2.name::text ---Adds admin 2 labels using the admin name column
left join csv.admin3 a3 on srcaf.admin3=a3.name::text ---Adds admin 3 labels using the admin name column
left join csv.admin4 a4 on srcaf.admin4=a4.name::text ---Adds admin 4 labels using the admin name column
left join csv.admin5 a5 on srcaf.admin5=a5.name::text ---Adds admin 5 labels using the admin name column
left join csv.barangay_gps bg on srcaf.admin4=bg.barangay_code::text
left join staging.rca_labels rl on rl.code=srcaf.vaccine_administered and rl.question='vaccine_administered'
)
select * from reasons_breakdown
where reasons_value is not null
);

alter view staging.rca_not_vaccinated_reasons owner to rt_vama;

-----PowerBI views
--- The PowerBI connector currently pulls data from views within the public schema. Hence the repointing to the public schema

---Supplemental Immunization Activity Form
--SIA actual values
create or replace view public.sia_actuals as 
(
select * from staging.sia_actuals
);
alter view public.sia_actuals owner to rt_vama;

---SIA Aggregated values 
create or replace view public.aggregated_sia_actuals_target as 
(
select * from staging.aggregated_sia_actuals_target
);
alter view public.aggregated_sia_actuals_target owner to rt_vama;

---SIA Actuals, Target
create or replace view public.sia_actuals_targets
(
select * from staging.sia_actuals_target sat 
);
alter view public.sia_actuals_targets owner to rt_vama;


----SIA reasons breakdown
create or replace view public.sia_deferred_refused_reasons as 
(
select * from staging.deferred_refused_reasons drr 
);
alter view public.sia_deferred_refused_reasons owner to rt_vama;

----SIA Records
create or replace view public.sia_records as 
(
select * from staging.sia_records
);
alter view public.sia_records owner to rt_vama;

----Social mobilization indicators
create or replace view public.social_mobilization_indicators as 
(
select * from staging.social_mobilization_indicators
);
alter view public.social_mobilization_indicators owner to rt_vama;


---Monitoring tool
create or replace view public.monitoring_tool as 
(
select * from staging.monitoring_tool mt 
);
alter view public.monitoring_tool owner to rt_vama;

----Monitored facilities overall proportion
create or replace view public.monitored_facilities_overall_proportion as 
(
select * from staging.monitored_facilities_overall_proportion
);
alter view public.monitored_facilities_overall_proportion owner to rt_vama;

-----Vaccinated children under the monitoing tool
create or replace view public.vaccinated_children_under_monitoring_tool as 
(
select * from staging.vaccinated_children_under_monitoring_tool
);
alter view public.vaccinated_children_under_monitoring_tool owner to rt_vama;

----Health Assessment Readiness
create or replace view public.hcl_monitoring_assessment as 
(
select * from staging.hcl_monitoring_assessment
);
alter view public.hcl_monitoring_assessment owner to rt_vama;

----Ready facilities
create or replace view public.ready_facilities as 
(
select * from staging.ready_facilities
);
alter view public.ready_facilities owner to rt_vama;


-----RCA
create or replace view public.rca_actuals as 
(
select * from staging.rca_actuals
);
alter view public.rca_actuals owner to rt_vama;

create or replace view public.rca as 
(
select * from staging.rca
);
alter view public.rca owner to rt_vama;

create or replace view public.rca_sources_info as 
(
select * from staging.rca_sources_info
);
alter view public.rca_sources_info owner to rt_vama;


create or replace view public.rca_not_vaccinated_reasons as 
(
select * from staging.rca_not_vaccinated_reasons
);
alter view public.rca_not_vaccinated_reasons owner to rt_vama;


