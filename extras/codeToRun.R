#installation
install.packages("D:/Users/hungd/Documents/OMOP_CDM/git_folders/DataQualityFlag",
repos = NULL,
type = "source",
INSTALL_opts=c("--no-multiarch")
)


library(DataQualityDashboard)


# fill out the connection details -----------------------------------------------------------------------
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql", user = "postgres", 
                                                                password = "postgres12", server = "localhost/dream", 
                                                                port = "5432", extraSettings = "")

cdmDatabaseSchema <- "public" # the fully qualified database schema name of the CDM
resultsDatabaseSchema <- "result_dqd" # the fully qualified database schema name of the results schema (that you can write to)
cdmSourceName <- "flag_dream" # a human readable name for your CDM source

# determine how many threads (concurrent SQL sessions) to use ----------------------------------------
numThreads <- 1#2 # on Redshift, 3 seems to work well

# specify if you want to execute the queries or inspect them ------------------------------------------
sqlOnly <- FALSE # set to TRUE if you just want to get the SQL scripts and not actually run the queries

# where should the logs go? -------------------------------------------------------------------------
outputFolder <- "D:/Users/hungd/Documents/OMOP_CDM/git_folders/DataQualityDashboard-master/output"

# logging type -------------------------------------------------------------------------------------
verboseMode <- TRUE #FALSE # set to TRUE if you want to see activity written to the console

# write results to table? -----------------------------------------------------------------------
writeToTable <- TRUE #FALSE # set to FALSE if you want to skip writing to results table

# if writing to table and using Redshift, bulk loading can be initialized -------------------------------

# Sys.setenv("AWS_ACCESS_KEY_ID" = "",
#            "AWS_SECRET_ACCESS_KEY" = "",
#            "AWS_DEFAULT_REGION" = "",
#            "AWS_BUCKET_NAME" = "",
#            "AWS_OBJECT_KEY" = "",
#            "AWS_SSE_TYPE" = "AES256",
#            "USE_MPP_BULK_LOAD" = TRUE)

# which DQ check levels to run -------------------------------------------------------------------
checkLevels <- c("FIELD")  #("TABLE", "FIELD", "CONCEPT")

# which DQ checks to run? ------------------------------------

checkNames <- c("plausibleValueLow") #c() #Names can be found in inst/csv/OMOP_CDM_v5.3.1_Check_Desciptions.csv

# which CDM tables to exclude? ------------------------------------
#tablesToExclude <- c()
tablesToExclude <- c("CONDITION_ERA", "DRUG_ERA", "DOSE_ERA","OBSERVATION_PERIOD","VISIT_OCCURRENCE", "DRUG_EXPOSURE","PROCEDURE_OCCURRENCE","DEVICE_EXPOSURE","VISIT_DETAIL","NOTE","NOTE_NLP","OBSERVATION","SPECIMEN","MEASUREMENT") #c()

# run the job --------------------------------------------------------------------------------------
DataQualityFlag::executeDqChecks(connectionDetails = connectionDetails, 
                              cdmDatabaseSchema = cdmDatabaseSchema, 
                              resultsDatabaseSchema = resultsDatabaseSchema,
                              cdmSourceName = cdmSourceName, 
                              numThreads = numThreads,
                              sqlOnly = sqlOnly, 
                              outputFolder = outputFolder, 
                              verboseMode = verboseMode,
                              writeToTable = writeToTable,
                              checkLevels = checkLevels,
                              tablesToExclude = tablesToExclude,
                              checkNames = checkNames)

# inspect logs ----------------------------------------------------------------------------
ParallelLogger::launchLogViewer(logFileName = file.path(outputFolder, cdmSourceName, 
                                                        sprintf("log_DqDashboard_%s.txt", cdmSourceName)))

# (OPTIONAL) if you want to write the JSON file to the results table separately -----------------------------
jsonFilePath <- "D:/Users/hungd/Documents/OMOP_CDM/git_folders/DataQualityDashboard-master/output/write_to_database_synthea"
DataQualityDashboard::writeJsonResultsToTable(connectionDetails = connectionDetails, 
                                              resultsDatabaseSchema = resultsDatabaseSchema, 
                                              jsonFilePath = jsonFilePath)

DataQualityDashboard::viewDqDashboard(file.path(getwd(),outputFolder, cdmSourceName, paste0("results_", cdmSourceName, ".json")))
