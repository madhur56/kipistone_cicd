
CREATE SCHEMA STG_BRZ;

CREATE OR REPLACE STAGE AWS_POSITIONS_STAGE   
url='s3://data-stream-demo/api-data/positions.json'
credentials=(aws_key_id='AKIARYOVO3DIJJNKI6FI' aws_secret_key='5IClbuVqy+5X2uFf0dJRNm7CpdYNd4nMYHZYvuIx');


list @AWS_POSITIONS_STAGE;

--Create File Format for JSON DATA

CREATE OR REPLACE FILE FORMAT JSON_FILE_FORMAT_POS
TYPE = 'JSON' 
COMPRESSION = 'AUTO' 
ENABLE_OCTAL = FALSE 
ALLOW_DUPLICATE = FALSE 
STRIP_OUTER_ARRAY = TRUE 
STRIP_NULL_VALUES = FALSE 
IGNORE_UTF8_ERRORS = FALSE;


create or replace table positions as
select  
  $1:_id::string AS _id,
  $1:type:id::string AS type_id,
  $1:type:name::string AS type_name,
  $1:state::string AS state,
  $1:name::string AS name,
  $1:friendly_id::string AS friendly_id,
  $1:experience::string AS experience,
  $1:location:city::string AS location_city,
  $1:location:name::string AS location_name,
  $1:location:country:name::string AS location_country_name,
  $1:location:country:id::string AS location_country_id,
  $1:education::string AS education,
  $1:department::string AS department,
  $1:requisition_id::string AS requisition_id,
  $1:category::string AS category,
  $1:application_form:name::string AS application_form_name,
  $1:application_form:headline::string AS application_form_headline,
  $1:application_form:summary::string AS application_form_summary,
  $1:application_form:profile_photo::string AS application_form_profile_photo,
  $1:application_form:address::string AS application_form_address,
  $1:application_form:email_address::string AS application_form_email_address,
  $1:application_form:phone_number::string AS application_form_phone_number,
  $1:application_form:resume::string AS application_form_resume,
  $1:application_form:work_history::string AS application_form_work_history,
  $1:application_form:education::string AS application_form_education,
  $1:application_form:cover_letter::string AS application_form_cover_letter,
  $1:application_form:salary::string AS application_form_salary,
  $1:application_form:questionnaire_in_experience::boolean AS application_form_questionnaire_in_experience,
  $1:creator_id::string AS creator_id,
  $1:creation_date::timestamp AS creation_date,
  $1:updated_date::timestamp AS updated_date,
  $1:all_users::array AS all_users,
  $1:all_admins::array AS all_admins,
  $1:pipeline_id::string AS pipeline_id,
  $1:candidate_type::string AS candidate_type,
  $1:org_type::string AS org_type,
  $1:bias_enabled::boolean AS bias_enabled,
  $1:description::string AS description,
  CURRENT_TIMESTAMP AS current_timestamp
  from @AWS_POSITIONS_STAGE
(FILE_FORMAT => 'JSON_FILE_FORMAT_POS',
pattern => '.*positions.*[.]json');
