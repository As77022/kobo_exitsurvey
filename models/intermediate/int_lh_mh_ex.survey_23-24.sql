

--just modify the AcademicYear and class as per requirement
WITH new_table  AS
(
SELECT DISTINCT "scm"."StudentId" AS ExitStudentId,
	--Academic Information
	"scm"."AcademicYearId", "ay"."YearName" AS AcademicYear, "vtp"."VTPName", "vc"."FullName" AS VCName, "vt"."FullName" AS VTName, 
    "vt"."Mobile" AS VTMobile, "sec"."SectorName" AS Sector, "jr"."JobRoleName" AS JobRole, "st"."StateName" AS State, 
	"dvs"."DivisionName" AS Division, "ds"."DistrictName" AS District, "s"."SchoolName" AS NameOfSchool, "s"."UDISE" AS UdiseCode, "scl"."Name" AS Class,
	-- Personal Information
	"ex"."SeatNo", "scd"."StudentRollNumber" AS StudentUniqueId, "sc"."FirstName" AS StudentFirstName, "sc"."MiddleName" AS StudentMiddleName, 
	"sc"."LastName" AS StudentLastName, "sc"."FullName" AS StudentFullName, "dv"."Name" AS Gender, "scd"."DateOfBirth" AS DOB, "scd"."FatherName",
	"scd"."MotherName", "dv1"."Name" AS Category, "ex"."Religion", "dvsm"."Name" AS StreamName, "scd"."Mobile" AS StudentMobileNo, "scd"."WhatsAppNo" AS StudentWhatsAppNo,
	"scd"."Mobile1" AS ParentMobileNo,
	-- Residential Information		
	"ex"."CityOfResidence", "ex"."DistrictOfResidence", "ex"."BlockOfResidence", "ex"."PinCode", "ex"."StudentAddress",
    -- Education post 10/12 th      
	"ex"."DoneInternship", "ex"."InternshipCompletedSector", "ex"."WillContHigherStudies", "ex"."IsFullTime", "ex"."CourseToPursue",       
	"ex"."OtherCourse", "ex"."StreamOfEducation", "ex"."OtherStreamStudying", "ex"."WillContVocEdu", "ex"."WillContVocational11",
	"ex"."ReasonsNOTToContinue", "ex"."WillContSameSector", "ex"."SectorForTraining", "ex"."OtherSector",
			
	-- Employment Details        
	"ex"."CurrentlyEmployed", "ex"."WorkTitle", "ex"."DetailsOfEmployment",	"ex"."SectorsOfEmployment", "ex"."IsVSCompleted",
			
	-- Support
	"ex"."WantToPursueAnySkillTraining", "ex"."IsFulltimeWillingness", "ex"."HveRegisteredOnEmploymentPortal", "ex"."EmploymentPortalName",
	"ex"."WillingToGetRegisteredOnNAPS", "ex"."IntrestedInJobOrSelfEmploymentPost12th", "ex"."PreferredLocations", "ex"."ParticularLocation",
	"ex"."WantToKnowAboutOpportunities", "ex"."CanLahiGetInTouch", "ex"."WantToKnowAbtPgmsForJobsNContEdu",        
			
	-- Status & Remarks
	"ex"."CollectedEmailId", "ex"."SurveyCompletedByStudentORParent", "ex"."DateOfIntv", "ex"."Remark",
	CASE 
    WHEN "ex"."ExitStudentId" IS NOT NULL THEN 'Submitted' 
    ELSE '' 
END AS "ExitSurveyStatus", "ex"."IsRelevantToVocCourse",
	"ex"."WillBeFullTime", "ex"."OtherReasons", "ex"."DoesFieldStudyHveVocSub", "ex"."InterestedInJobOrSelfEmployment",
	"ex"."TopicsOfInterest", "ex"."AnyPreferredLocForEmployment", "ex"."CanSendTheUpdates", "ex"."StudentWANo",
	"ex"."WillingToContSkillTraining", "ex"."CourseForTraining", "ex"."CourseNameIfOther", "ex"."SkillTrainingType",                
	"ex"."OtherSectorsIfAny", "ex"."WantToKnowAbtSkillsUnivByGvt", "ex"."TrainingType", "ex"."SectorForSkillTraining",
	"ex"."OthersIfAny", "ex"."WillingToGoForTechHighEdu", "ex"."DifferentProgramOpportunities", 
	TRUE AS IsAssessmentRequired, "scd"."AssessmentConducted", "ex"."CreatedOn" AS SubmissionDate    
    
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
SELECT count(*) FROM new_table
WHERE Class IN ('Class 10','Class 12')