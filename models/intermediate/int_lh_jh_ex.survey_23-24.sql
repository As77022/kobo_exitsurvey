--just modify the AcademicYear and class as per requirement
WITH temp_table  AS
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
    
	FROM {{source('LH_JH_exitsurvey','StudentClassMapping')}} AS scm  
	INNER JOIN {{source('LH_JH_exitsurvey','StudentClasses')}} AS sc ON "scm"."SchoolId" = "sc"."SchoolId" AND "scm"."SectionId" = "sc"."SectionId" AND "scm"."StudentId" = "sc"."StudentId" AND "sc"."DateOfDropout" IS NULL AND "sc"."IsActive" = TRUE
	INNER JOIN {{source('LH_JH_exitsurvey','VTSchoolSectors')}} AS vss ON "scm"."AcademicYearId" = "vss"."AcademicYearId" AND "scm"."SchoolId" = "vss"."SchoolId" AND "scm"."VTId" = "vss"."VTId" AND "vss"."DateOfRemoval" IS NULL AND "vss"."IsActive" = TRUE   
	INNER JOIN {{source('LH_JH_exitsurvey','Sectors')}} AS sec ON "vss"."SectorId" = "sec"."SectorId" AND "sec"."IsActive" = TRUE
	INNER JOIN {{source('LH_JH_exitsurvey','JobRoles')}} AS jr ON "vss"."SectorId" = "jr"."SectorId" AND "vss"."JobRoleId" = "jr"."JobRoleId" AND "jr"."IsActive" = TRUE
	INNER JOIN {{source('LH_JH_exitsurvey','Schools')}} AS s ON "scm"."SchoolId" = "s"."SchoolId" AND "s"."IsActive" = TRUE
	INNER JOIN {{source('LH_JH_exitsurvey','DataValues')}} AS dv ON "sc"."Gender" = "dv"."DataValueId" AND "dv"."DataTypeId" = 'StudentGender' 	
	INNER JOIN {{source('LH_JH_exitsurvey','SchoolClasses')}} AS scl ON "scm"."ClassId" = "scl"."ClassId" AND "scl"."IsActive" = TRUE
	INNER JOIN {{source('LH_JH_exitsurvey','VCTrainersMap')}} AS vtm ON "scm"."AcademicYearId" = "vtm"."AcademicYearId" AND "scm"."VTId" = "vtm"."VTId" AND "vtm"."DateOfResignation" IS NULL AND "vtm"."IsActive" = TRUE
	INNER JOIN {{source('LH_JH_exitsurvey','VocationalTrainers')}} AS vt ON "vtm"."VTId" = "vt"."VTId" AND "vt"."IsActive" = TRUE
	INNER JOIN {{source('LH_JH_exitsurvey','VocationalCoordinators')}} AS vc ON "vtm"."VCId" = "vc"."VCId" AND "vc"."IsActive" = TRUE
	INNER JOIN {{source('LH_JH_exitsurvey','VocationalTrainingProviders')}} AS vtp ON "vtm"."VTPId" = "vtp"."VTPId" AND "vtp"."IsActive" = TRUE
	INNER JOIN {{source('LH_JH_exitsurvey','AcademicYears')}} AS ay ON "scm"."AcademicYearId" = "ay"."AcademicYearId" AND "ay"."YearName" = '2023-2024' -- (modify the AcademicYear as per requirement)
	INNER JOIN {{source('LH_JH_exitsurvey','States')}} AS st ON "s"."StateCode" = "st"."StateCode" AND "st"."IsActive" = TRUE
	INNER JOIN {{source('LH_JH_exitsurvey','Divisions')}} AS dvs ON "s"."DivisionId" = "dvs"."DivisionId" AND "dvs"."IsActive" = TRUE
	INNER JOIN {{source('LH_JH_exitsurvey','Districts')}} AS ds ON "s"."DivisionId" = "ds"."DivisionId" AND "s"."DistrictCode" = "ds"."DistrictCode" AND "ds"."IsActive" = TRUE
	LEFT JOIN {{source('LH_JH_exitsurvey','StudentClassDetails')}} AS scd ON "sc"."StudentId" = "scd"."StudentId" 
	LEFT JOIN {{source('LH_JH_exitsurvey','DataValues')}} AS dv1 ON "scd"."SocialCategory" = "dv1"."DataValueId" AND "dv1"."DataTypeId" = 'SocialCategory' 		 	    	
	LEFT JOIN {{source('LH_JH_exitsurvey','DataValues')}} AS dvsm ON "scd"."StreamId" = "dvsm"."DataValueId" AND "dvsm"."DataTypeId" = 'Streams'
	LEFT JOIN {{source('LH_JH_exitsurvey','ExitSurveyDetails')}} AS ex ON "scm"."AcademicYearId" = "ex"."AcademicYearId" AND "scm"."StudentId" = "ex"."ExitStudentId"
)
SELECT count(*) FROM temp_table
WHERE Class IN ('Class 10','Class 12')