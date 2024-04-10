

WITH cte AS (
    (SELECT DISTINCT * FROM {{ref("int_ap_10th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_ap_12th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_cg_10th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_cg_12th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_hp_10th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_hp_12th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_ka_10th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_ka_12th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_ng_10th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_ng_12th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_pb_10th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_pb_12th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_tl_10th_ex.survey_23-24")}})
    UNION ALL
    (SELECT DISTINCT * FROM {{ref("int_tl_12th_ex.survey_23-24")}})
)
SELECT
    cast("start_date" as TIMESTAMP),
    CAST("end_date" as TIMESTAMP),
    "today":: DATE	,
    "state"	,
    "academic_year"	,
    "district"	,
    "vtp_name"	,
    "school_udise"	,
    "school_name"	,
    "select_sector"	,
    "vt_name"	,
    "vt_mobile_number"	,
    "sector"	,
    "class"	,
    "job_role"	,
    "id"	,
    "uuid"	,
	CAST("submission_time" AS TIMESTAMP),
    "index"	,
    "first_name"	,
    "middle_name"	,
    "last_name"	,
    "fullname"	,
    "date_of_birth":: DATE	,
    "age"	,
    "gender"	,
    "student_unique_id"	,
    "student_unique_id_new"	,
    "father_name"	,
    "mother_name"	,
    "primary_contact_number"	,
    "alternate_number_preferably_whatsapp"	,
    "stream_of_education"	,
    "specify_other_stream"	,
    "date_of_exit_interview"::DATE	,
    "completed_ojt_internship_in_11_12"	,
    "firm_organisation_name"	,
    "firm_contact_number"	,
    "firm_city_location"	,
    "details_ojt_internship_employer"	,
    "would_you_conti_high_edu"	,
    "which_course_will_you_pursue"	,
    "course_full_time_part_time"	,
    "other_course"	,
    "willing_to_conti_ve_in_11th"	,
    "reason_not_conti_high_educ"	,
    "reason_not_conti_high_educ_financial"	,
    "reason_not_conti_high_educ_need_a_job"	,
    "reason_not_conti_high_educ_family_not_allow"	,
    "reason_not_conti_high_educ_not_interested"	,
    "reason_not_conti_high_educ_failed_in_current"	,
    "reason_not_conti_high_educ_school_till_10th_only"	,
    "reason_not_conti_high_educ_marriage"	,
    "reason_not_conti_high_educ_other"	,
    "reasons_not_conti_ve_in_11th"	,
    "stream_of_higher_educ"	,
    "other_educ_stream_please_specify"	,
    "field_studies_contain_voc_subjects"	,
    "reasons_for_not_conti_ve"	,
    "other_reason_not_conti_ve_specify"	,
    "willing_to_conti_part_time_job_while_doing_educ"	,
    "are_you_currently_working_job_own_business_etc"	,
    "details_of_current_employment_title"	,
    "interested_in_job_or_self_employment_after_10_12"	,
    "kind_of_employment_are_you_interested_in"	,
    "preferred_location_for_employment"	,
    "particular_location_for_employment"	,
    "interested_in_skill_training"	,
    "interested_in_career_counseling"	,
    "concent_for_lahi_contact"	,
    "overall_ve_experience_in_school"	,
    "registered_on_naps_portal_for_apprentices"	,
    "are_you_intrested_in_doing_apprentice_naps_"	,
    "parent_index"	,
    "submission_id"	,
    "submission_uuid"
FROM cte
