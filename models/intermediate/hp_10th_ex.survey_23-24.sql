
WITH cte AS 
    (SELECT
    CAST(start AS TIMESTAMP) AS	"start_date"	,
	CAST("end" AS TIMESTAMP) AS "end_date",
    "today"::date	AS	"today"	,
    "state"::VARCHAR(5)	AS	"state"	,
    "academic_year"::VARCHAR(15)	AS	"academic_year"	,
    "district"::VARCHAR(20)	AS	"district"	,
    "vtp_name"::NAME	AS	"vtp_name"	,
    "school_udise"::VARCHAR(15)	AS	"school_udise"	,
    "school_name"	AS	"school_name"	,
    "select_sector"	AS	"select_sector"	,
    "vt_name"::NAME	AS	"vt_name"	,
    "vt_mobile_number"::varchar(13)	AS	"vt_mobile_number"	,
    "sector"	AS	"sector"	,
    "class"	AS	"class"	,
    "job_role"	AS	"job_role"	,
    "id"	AS	"id"	,
    "uuid"	AS	"uuid"	,
	CAST(submission_time AS TIMESTAMP) AS	"submission_date",
    "index"	AS	"index"	,
    "first_name"	AS	"first_name"	,
    "middle_name"	AS	"middle_name"	,
    "last_name"	AS	"last_name"	,
    "fullname"::NAME	AS	"fullname"	,
    "date_of_birth"::date	AS	"date_of_birth"	,
    "age"	AS	"age"	,
    "gender"	AS	"gender"	,
    "student_unique_id"	AS	"student_unique_id"	,
    "student_unique_id_new"	AS	"student_unique_id_new"	,
    "father_name"::name	AS	"father_name"	,
    "mother_name"::name	AS	"mother_name"	,
    "primary_contact_number"::varchar(13)	AS	"primary_contact_number"	,
    "alternate_number_preferably_whatsapp"::varchar(13)	AS	"alternate_number_preferably_whatsapp"	,
    "stream_of_education"	AS	"stream_of_education"	,
    "specify_other_stream"	AS	"specify_other_stream"	,
    "date_of_exit_interview"::DATE	AS	"date_of_exit_interview"	,
    "completed_ojt_internship_in_11_12_"	AS	"completed_ojt_internship_in_11_12"	,
    "firm_organisation_name"	AS	"firm_organisation_name"	,
    "firm_contact_number"::varchar(13)	AS	"firm_contact_number"	,
    "firm_city_location"	AS	"firm_city_location"	,
    "details_ojt_internship_employer"	AS	"details_ojt_internship_employer"	,
    "would_you_conti_high_edu"	AS	"would_you_conti_high_edu"	,
    "which_course_will_you_pursue_"	AS	"which_course_will_you_pursue"	,
    "course_full_time_part_time"	AS	"course_full_time_part_time"	,
    "other_course"	AS	"other_course"	,
    "willing_to_conti_ve_in_11_th_"	AS	"willing_to_conti_ve_in_11th"	,
    "reason_not_conti_high_educ_"	AS	"reason_not_conti_high_educ"	,
    "reason_not_conti_high_educ_financial"	AS	"reason_not_conti_high_educ_financial"	,
    "reason_not_conti_high_educ_need_a_job"	AS	"reason_not_conti_high_educ_need_a_job"	,
    "reason_not_conti_high_educ_family_not_allow"	AS	"reason_not_conti_high_educ_family_not_allow"	,
    "reason_not_conti_high_educ_not_interested"	AS	"reason_not_conti_high_educ_not_interested"	,
    "reason_not_conti_high_educ_failed_in_current"	AS	"reason_not_conti_high_educ_failed_in_current"	,
    "reason_not_conti_high_educ_school_till_10_th_only"	AS	"reason_not_conti_high_educ_school_till_10th_only"	,
    "reason_not_conti_high_educ_marriage"	AS	"reason_not_conti_high_educ_marriage"	,
    "reason_not_conti_high_educ_other"	AS	"reason_not_conti_high_educ_other"	,
    "reasons_not_conti_ve_in_11_th_"	AS	"reasons_not_conti_ve_in_11th"	,
    "stream_of_higher_educ"	AS	"stream_of_higher_educ"	,
    "other_educ_stream_please_specify_"	AS	"other_educ_stream_please_specify"	,
    "field_studies_contain_voc_subjects_"	AS	"field_studies_contain_voc_subjects"	,
    "reasons_for_not_conti_ve_"	AS	"reasons_for_not_conti_ve"	,
    "other_reason_not_conti_ve_specify_"	AS	"other_reason_not_conti_ve_specify"	,
    "willing_to_conti_part_time_job_while_doing_educ_"	AS	"willing_to_conti_part_time_job_while_doing_educ"	,
    "are_you_currently_working_job_own_business_etc_"	AS	"are_you_currently_working_job_own_business_etc"	,
    "details_of_current_employment_title"	AS	"details_of_current_employment_title"	,
    "interested_in_job_or_self_employment_after_10_12_"	AS	"interested_in_job_or_self_employment_after_10_12"	,
    "kind_of_employment_are_you_interested_in_"	AS	"kind_of_employment_are_you_interested_in"	,
    "preferred_location_for_employment"	AS	"preferred_location_for_employment"	,
    "particular_location_for_employment"	AS	"particular_location_for_employment"	,
    "interested_in_skill_training_"	AS	"interested_in_skill_training"	,
    "interested_in_career_counseling_"	AS	"interested_in_career_counseling"	,
    "concent_for_lahi_contact"	AS	"concent_for_lahi_contact"	,
    "overall_ve_experience_in_school_"	AS	"overall_ve_experience_in_school"	,
    "registered_on_naps_portal_for_apprentices"	AS	"registered_on_naps_portal_for_apprentices"	,
    "are_you_intrested_in_doing_apprentice_naps_"	AS	"are_you_intrested_in_doing_apprentice_naps_"	,
    "parent_index"	AS	"parent_index"	,
    "submission_id"	AS	"submission_id"	,
    "submission_uuid"	AS	"submission_uuid"
FROM {{source('kobo_exitsurvey','hp_10th_exitsurvey_kobo')}})   
SELECT * FROM cte
