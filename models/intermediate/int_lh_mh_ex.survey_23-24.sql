--just modify the AcademicYear and class as per requirement
WITH new_table  AS
(
SELECT DISTINCT "scm"."StudentId" AS ExitStudentId,
	--Academic Information
	"scm"."AcademicYearId", "ay"."YearName" AS AcademicYear, "vtp"."VTPName" AS VTPName, "vc"."FullName" AS VCName, "vt"."FullName" AS VTName, 
    "vt"."Mobile" AS VTMobile, "sec"."SectorName" AS Sector, "jr"."JobRoleName" AS JobRole, "st"."StateName" AS State, 
	"dvs"."DivisionName" AS Division, "ds"."DistrictName" AS District, "s"."SchoolName" AS NameOfSchool, "s"."UDISE" AS UdiseCode, "scl"."Name" AS Class,
	-- Personal Information
	"ex"."SeatNo" AS SeatNo , "scd"."StudentRollNumber" AS StudentUniqueId, "sc"."FirstName" AS StudentFirstName, "sc"."MiddleName" AS StudentMiddleName, 
	"sc"."LastName" AS StudentLastName, "sc"."FullName" AS StudentFullName, "dv"."Name" AS Gender, "scd"."DateOfBirth" AS DOB, "scd"."FatherName" AS FatherName,
	"scd"."MotherName" AS MotherName, "dv1"."Name" AS Category, "ex"."Religion" AS Religion, "dvsm"."Name" AS StreamName, "scd"."Mobile" AS StudentMobileNo, "scd"."WhatsAppNo" AS StudentWhatsAppNo,
	"scd"."Mobile1" AS ParentMobileNo,
	-- Residential Information		
	"ex"."CityOfResidence" AS CityOfResidence, "ex"."DistrictOfResidence" AS DistrictOfResidence, "ex"."BlockOfResidence" AS BlockOfResidence, "ex"."PinCode" AS PinCode, "ex"."StudentAddress" AS StudentAddress,
    -- Education post 10/12 th      
	"ex"."DoneInternship" AS DoneInternship, "ex"."InternshipCompletedSector" AS InternshipCompletedSector, "ex"."WillContHigherStudies" AS WillContHigherStudies, "ex"."IsFullTime" AS IsFullTime, "ex"."CourseToPursue" AS CourseToPursue,       
	"ex"."OtherCourse" AS OtherCourse, "ex"."StreamOfEducation" AS StreamOfEducation, "ex"."OtherStreamStudying" AS OtherStreamStudying,
    "ex"."WillContVocEdu" AS WillContVocEdu, "ex"."WillContVocational11" AS WillContVocational11,
	"ex"."ReasonsNOTToContinue" AS ReasonsNOTToContinue, "ex"."WillContSameSector" AS WillContSameSector,"ex"."SectorForTraining" AS SectorForTraining, "ex"."OtherSector" AS OtherSector,
			
	-- Employment Details        
	"ex"."CurrentlyEmployed" AS CurrentlyEmployed, "ex"."WorkTitle" AS WorkTitle, "ex"."DetailsOfEmployment" AS	DetailsOfEmployment,"ex"."SectorsOfEmployment" AS SectorsOfEmployment, "ex"."IsVSCompleted" AS IsVSCompleted,
			
	-- Support
	"ex"."WantToPursueAnySkillTraining" AS WantToPursueAnySkillTraining, "ex"."IsFulltimeWillingness" AS IsFulltimeWillingness, "ex"."HveRegisteredOnEmploymentPortal" AS HveRegisteredOnEmploymentPortal, "ex"."EmploymentPortalName" AS EmploymentPortalName,
	"ex"."WillingToGetRegisteredOnNAPS" AS WillingToGetRegisteredOnNAPS, "ex"."IntrestedInJobOrSelfEmploymentPost12th" AS IntrestedInJobOrSelfEmploymentPost12th,"ex"."PreferredLocations" AS PreferredLocations, "ex"."ParticularLocation" AS ParticularLocation,
	"ex"."WantToKnowAboutOpportunities" AS WantToKnowAboutOpportunities, "ex"."CanLahiGetInTouch" AS CanLahiGetInTouch, "ex"."WantToKnowAbtPgmsForJobsNContEdu" AS WantToKnowAbtPgmsForJobsNContEdu,       
			
	-- Status & Remarks
	"ex"."CollectedEmailId" AS CollectedEmailId, "ex"."SurveyCompletedByStudentORParent" AS SurveyCompletedByStudentORParent, "ex"."DateOfIntv" AS DateOfIntv, "ex"."Remark" AS Remark,
	CASE 
    WHEN "ex"."ExitStudentId" IS NOT NULL THEN 'Submitted' 
    ELSE '' 
    END AS ExitSurveyStatus , "ex"."IsRelevantToVocCourse" AS IsRelevantToVocCourse,
	"ex"."WillBeFullTime" AS WillBeFullTime, "ex"."OtherReasons" AS OtherReasons, "ex"."DoesFieldStudyHveVocSub" AS DoesFieldStudyHveVocSub, "ex"."InterestedInJobOrSelfEmployment" AS InterestedInJobOrSelfEmployment,
	"ex"."TopicsOfInterest" AS TopicsOfInterest, "ex"."AnyPreferredLocForEmployment" AS AnyPreferredLocForEmployment, "ex"."CanSendTheUpdates" AS CanSendTheUpdates, "ex"."StudentWANo" AS StudentWANo,
	"ex"."WillingToContSkillTraining" AS WillingToContSkillTraining, "ex"."CourseForTraining" AS CourseForTraining, "ex"."CourseNameIfOther" AS CourseNameIfOther,"ex"."SkillTrainingType" AS SkillTrainingType,                
	"ex"."OtherSectorsIfAny" AS OtherSectorsIfAny, "ex"."WantToKnowAbtSkillsUnivByGvt" AS WantToKnowAbtSkillsUnivByGvt, "ex"."TrainingType" AS TrainingType, "ex"."SectorForSkillTraining" AS SectorForSkillTraining,
	"ex"."OthersIfAny" AS OthersIfAny, "ex"."WillingToGoForTechHighEdu" AS WillingToGoForTechHighEdu, "ex"."DifferentProgramOpportunities" AS DifferentProgramOpportunities,
	TRUE AS IsAssessmentRequired, "scd"."AssessmentConducted" AS AssessmentConducted, "ex"."CreatedOn" AS SubmissionDate
    
	FROM {{source('LH_MH_exitsurvey','StudentClassMapping')}} AS scm  
	INNER JOIN {{source('LH_MH_exitsurvey','StudentClasses')}} AS sc ON "scm"."SchoolId" = "sc"."SchoolId" AND "scm"."SectionId" = "sc"."SectionId" AND "scm"."StudentId" = "sc"."StudentId" AND "sc"."DateOfDropout" IS NULL AND "sc"."IsActive" = TRUE
	INNER JOIN {{source('LH_MH_exitsurvey','VTSchoolSectors')}} AS vss ON "scm"."AcademicYearId" = "vss"."AcademicYearId" AND "scm"."SchoolId" = "vss"."SchoolId" AND "scm"."VTId" = "vss"."VTId" AND "vss"."DateOfRemoval" IS NULL AND "vss"."IsActive" = TRUE   
	INNER JOIN {{source('LH_MH_exitsurvey','Sectors')}} AS sec ON "vss"."SectorId" = "sec"."SectorId" AND "sec"."IsActive" = TRUE
	INNER JOIN {{source('LH_MH_exitsurvey','JobRoles')}} AS jr ON "vss"."SectorId" = "jr"."SectorId" AND "vss"."JobRoleId" = "jr"."JobRoleId" AND "jr"."IsActive" = TRUE
	INNER JOIN {{source('LH_MH_exitsurvey','Schools')}} AS s ON "scm"."SchoolId" = "s"."SchoolId" AND "s"."IsActive" = TRUE
	INNER JOIN {{source('LH_MH_exitsurvey','DataValues')}} AS dv ON "sc"."Gender" = "dv"."DataValueId" AND "dv"."DataTypeId" = 'StudentGender' 	
	INNER JOIN {{source('LH_MH_exitsurvey','SchoolClasses')}} AS scl ON "scm"."ClassId" = "scl"."ClassId" AND "scl"."IsActive" = TRUE
	INNER JOIN {{source('LH_MH_exitsurvey','VCTrainersMap')}} AS vtm ON "scm"."AcademicYearId" = "vtm"."AcademicYearId" AND "scm"."VTId" = "vtm"."VTId" AND "vtm"."DateOfResignation" IS NULL AND "vtm"."IsActive" = TRUE
	INNER JOIN {{source('LH_MH_exitsurvey','VocationalTrainers')}} AS vt ON "vtm"."VTId" = "vt"."VTId" AND "vt"."IsActive" = TRUE
	INNER JOIN {{source('LH_MH_exitsurvey','VocationalCoordinators')}} AS vc ON "vtm"."VCId" = "vc"."VCId" AND "vc"."IsActive" = TRUE
	INNER JOIN {{source('LH_MH_exitsurvey','VocationalTrainingProviders')}} AS vtp ON "vtm"."VTPId" = "vtp"."VTPId" AND "vtp"."IsActive" = TRUE
	INNER JOIN {{source('LH_MH_exitsurvey','AcademicYears')}} AS  ay ON "scm"."AcademicYearId" = "ay"."AcademicYearId" AND "ay"."YearName" = '2023-2024' -- (modify the AcademicYear as per requirement)
	INNER JOIN {{source('LH_MH_exitsurvey','States')}} AS st ON "s"."StateCode" = "st"."StateCode" AND "st"."IsActive" = TRUE
	INNER JOIN {{source('LH_MH_exitsurvey','Divisions')}} AS dvs ON "s"."DivisionId" = "dvs"."DivisionId" AND "dvs"."IsActive" = TRUE
	INNER JOIN {{source('LH_MH_exitsurvey','Districts')}} AS ds ON "s"."DivisionId" = "ds"."DivisionId" AND "s"."DistrictCode" = "ds"."DistrictCode" AND "ds"."IsActive" = TRUE
	LEFT JOIN {{source('LH_MH_exitsurvey','StudentClassDetails')}} AS scd ON "sc"."StudentId" = "scd"."StudentId" 
	LEFT JOIN {{source('LH_MH_exitsurvey','DataValues')}} AS dv1 ON "scd"."SocialCategory" = "dv1"."DataValueId" AND "dv1"."DataTypeId" = 'SocialCategory' 		 	    	
	LEFT JOIN {{source('LH_MH_exitsurvey','DataValues')}} AS dvsm ON "scd"."StreamId" = "dvsm"."DataValueId" AND "dvsm"."DataTypeId" = 'Streams'
	LEFT JOIN {{source('LH_MH_exitsurvey','ExitSurveyDetails')}} AS ex ON "scm"."AcademicYearId" = "ex"."AcademicYearId" AND "scm"."StudentId" = "ex"."ExitStudentId"
)
SELECT 
	"nt".ExitStudentId AS ExitStudentId ,
	"nt".AcademicYear AS AcademicYear,
	"nt".VTPName AS VTPName,
	"nt".VCName AS VCName,
	"nt".VTName AS VTName,
	"nt".VTMobile AS VTMobile,
	"nt".Sector AS Sector,
	"nt".JobRole AS JobRole,
	"nt".State AS State,
	"nt".Division AS Division,
	"nt".District AS District,
	"nt".NameOfSchool AS NameOfSchool,
	"nt".UdiseCode AS UdiseCode,
	"nt".Class AS Class,
	"nt".SeatNo AS SeatNo,
	"nt".StudentUniqueId AS StudentUniqueId,
	"nt".StudentFirstName AS StudentFirstName,
	"nt".StudentMiddleName AS StudentMiddleName,
	"nt".StudentLastName AS StudentLastName,
	"nt".StudentFullName AS StudentFullName,
	"nt".Gender AS Gender,
	"nt".DOB::date AS DOB,
	"nt".FatherName AS FatherName,
	"nt".MotherName AS MotherName,
	"nt".Category AS Category,
	"nt".Religion AS Religion,
	"nt".StreamName AS StreamName,
	"nt".StudentMobileNo AS StudentMobileNo,
	"nt".StudentWhatsAppNo AS StudentWhatsAppNo,
	"nt".ParentMobileNo AS ParentMobileNo,
	"nt".CityOfResidence AS CityOfResidence,
	"nt".DistrictOfResidence AS DistrictOfResidence , 
	"nt".BlockOfResidence AS BlockOfResidence, 
	"nt".StudentAddress AS StudentAddress,
	"nt".DoneInternship AS DoneInternship,
	"nt".InternshipCompletedSector AS InternshipCompletedSector,
	"nt".WillContHigherStudies AS WillContHigherStudies,
	"nt".IsFullTime AS IsFullTime,
	"nt".CourseToPursue AS CourseToPursue,      
	"nt".OtherCourse AS OtherCourse,
	"nt".StreamOfEducation AS StreamOfEducation,
	"nt".OtherStreamStudying AS OtherStreamStudying,
	"nt".WillContVocEdu AS WillContVocEdu,
	"nt".WillContVocational11 AS WillContVocational11,
	"nt".ReasonsNOTToContinue AS ReasonsNOTToContinue,
	"nt".WillContSameSector AS WillContSameSector,
	"nt".SectorForTraining AS SectorForTraining,
	"nt".OtherSector AS OtherSector,
	"nt".CurrentlyEmployed AS CurrentlyEmployed,
	"nt".WorkTitle AS WorkTitle,
	"nt".DetailsOfEmployment AS DetailsOfEmployment,
	"nt".SectorsOfEmployment AS SectorsOfEmployment,
	"nt".IsVSCompleted AS IsVSCompleted,
	"nt".WantToPursueAnySkillTraining AS WantToPursueAnySkillTraining,
	"nt".IsFulltimeWillingness AS IsFulltimeWillingness,
	"nt".HveRegisteredOnEmploymentPortal AS HveRegisteredOnEmploymentPortal,
	"nt".EmploymentPortalName AS EmploymentPortalName,
	"nt".WillingToGetRegisteredOnNAPS AS WillingToGetRegisteredOnNAPS,
	"nt".IntrestedInJobOrSelfEmploymentPost12th AS IntrestedInJobOrSelfEmploymentPost12th,
	"nt".PreferredLocations AS PreferredLocations,
	"nt".ParticularLocation AS ParticularLocation,
	"nt".WantToKnowAboutOpportunities AS WantToKnowAboutOpportunities,
	"nt".CanLahiGetInTouch AS CanLahiGetInTouch,
	"nt".WantToKnowAbtPgmsForJobsNContEdu AS WantToKnowAbtPgmsForJobsNContEdu,
	"nt".CollectedEmailId AS CollectedEmailId,
	"nt".SurveyCompletedByStudentORParent AS SurveyCompletedByStudentORParent,
	"nt".DateOfIntv AS DateOfIntv,
	"nt".Remark AS Remark,
	"nt".ExitSurveyStatus AS ExitSurveyStatus,
	"nt".IsRelevantToVocCourse AS IsRelevantToVocCourse,
	"nt".WillBeFullTime AS WillBeFullTime,
	"nt".OtherReasons AS OtherReasons,
	"nt".DoesFieldStudyHveVocSub AS DoesFieldStudyHveVocSub, 
	"nt".InterestedInJobOrSelfEmployment AS InterestedInJobOrSelfEmployment,
	"nt".TopicsOfInterest AS TopicsOfInterest,
	"nt".AnyPreferredLocForEmployment AS AnyPreferredLocForEmployment,
	"nt".CanSendTheUpdates AS CanSendTheUpdates,
	"nt".StudentWANo AS StudentWANo,
	"nt".WillingToContSkillTraining AS WillingToContSkillTraining, 
	"nt".CourseForTraining AS CourseForTraining,
	"nt".CourseNameIfOther AS CourseNameIfOther,
	"nt".SkillTrainingType AS SkillTrainingType,             
	"nt".OtherSectorsIfAny AS OtherSectorsIfAny,
	"nt".WantToKnowAbtSkillsUnivByGvt AS WantToKnowAbtSkillsUnivByGvt,
	"nt".TrainingType AS TrainingType,
	"nt".SectorForSkillTraining AS SectorForSkillTraining,
	"nt".OthersIfAny AS OthersIfAny,
	"nt".WillingToGoForTechHighEdu AS WillingToGoForTechHighEdu,
	"nt".DifferentProgramOpportunities AS DifferentProgramOpportunities,
	"nt".IsAssessmentRequired AS IsAssessmentRequired,
	"nt".AssessmentConducted AS AssessmentConducted,
	"nt".SubmissionDate AS SubmissionDate,
    CAST(NULL AS TIMESTAMP) AS "start_date",
    CAST(NULL AS TIMESTAMP) AS "end_date",
    CAST(NULL AS date) AS "today",
    CAST(NULL AS VARCHAR(20)) AS "select_sector",
    CAST(NULL AS VARCHAR(20)) AS "id",
    CAST(NULL AS VARCHAR(20)) AS "uuid",
    CAST(NULL AS VARCHAR(20)) AS "index",
    CAST(NULL AS VARCHAR(20)) AS "student_unique_id_new",
    CAST(NULL AS VARCHAR(20)) AS "completed_ojt_internship_in_11_12",
    CAST(NULL AS VARCHAR(20)) AS "firm_organisation_name",
    CAST(NULL AS VARCHAR(20)) AS "firm_contact_number",
    CAST(NULL AS VARCHAR(20)) AS "firm_city_location",
    CAST(NULL AS VARCHAR(20)) AS "details_ojt_internship_employer",
    CAST(NULL AS VARCHAR(20)) AS "reason_not_conti_high_educ",
    CAST(NULL AS VARCHAR(20)) AS "reason_not_conti_high_educ_financial",
    CAST(NULL AS VARCHAR(20)) AS "reason_not_conti_high_educ_need_a_job",
    CAST(NULL AS VARCHAR(20)) AS "reason_not_conti_high_educ_family_not_allow",
    CAST(NULL AS VARCHAR(20)) AS "reason_not_conti_high_educ_not_interested",
    CAST(NULL AS VARCHAR(20)) AS "reason_not_conti_high_educ_failed_in_current",
    CAST(NULL AS VARCHAR(20)) AS "reason_not_conti_high_educ_school_till_10th_only",
    CAST(NULL AS VARCHAR(20)) AS "reason_not_conti_high_educ_marriage",
    CAST(NULL AS VARCHAR(20)) AS "reason_not_conti_high_educ_other",
    CAST(NULL AS VARCHAR(20)) AS "stream_of_higher_educ",
    CAST(NULL AS VARCHAR(20)) AS "stream_of_higher_educ",
    

other_educ_stream_please_specify
field_studies_contain_voc_subjects
reasons_for_not_conti_ve
other_reason_not_conti_ve_specify
willing_to_conti_part_time_job_while_doing_educ
kind_of_employment_are_you_interested_in
interested_in_career_counseling
overall_ve_experience_in_school
are_you_intrested_in_doing_apprentice_naps_
parent_index
submission_id
submission_uuid
FROM new_table as nt
WHERE Class IN ('Class 10','Class 12')
