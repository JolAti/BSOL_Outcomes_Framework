library(tidyverse)
library(DBI)        #database connection library
library(IMD)
library(PHEindicatormethods)

########################################################################################################
# Predefined variables
#Set age range which is used to return relevant population
########################################################################################################

#IndicatorID = 90810
#AgeMin = 0     # age >= AgeMin
#AgeMax = 18    # age <= AgeMax

#Demo_age = '0-18 yrs'
Demo_Gender = 'Persons'

########################################################################################################
# create connection
#
########################################################################################################

con <- dbConnect(odbc::odbc(), .connection_string = "Driver={SQL Server};server=MLCSU-BI-SQL;database=EAT_Reporting_BSOL", timeout = 10)

########################################################################################################
# Get OF model tables
#
########################################################################################################
# 
# query <- paste0(
#   "SELECT [IndicatorID]
#       ,[DomainID]
#       ,[ReferenceID]
#       ,[ICBIndicatorTitle]
#       ,[IndicatorLabel]
#       ,[StatusID]
#   FROM [EAT_Reporting_BSOL].[OF].[IndicatorList]
# ")
# 
# #query db for data
# table_indicatorID <- dbGetQuery(con, query)
# 
# query <- paste0(
#   "SELECT  [DemographicID]
#       ,[DemographicType]
#       ,[DemographicCode]
#       ,[DemographicLabel]
#   FROM [EAT_Reporting_BSOL].[OF].[Demographic]
# ")
# 
# #query db for data
# table_DemogrpahicID <- dbGetQuery(con, query)
# 
# query <- paste0(
#   "SELECT [AggregationID]
#       ,[AggregationType]
#       ,[AggregationCode]
#       ,[AggregationLabel]
#   FROM [EAT_Reporting_BSOL].[OF].[Aggregation]
# ")
# 
# #query db for data
# table_AggregationID <- dbGetQuery(con, query)

########################################################################################################
# Get data for indicator from warehouse
# Aggregation is LSOA to map to Wards and to IMD
# Grouped into financial years
# by Ethnic category using NHS 20 groups as letters
########################################################################################################
##extraction query
# query <- paste0("
#    select  count(*) as Numerator
# 		,null Denominator
# 		,g.EthnicCategoryCode
# 		,g.LowerLayerSuperOutputArea
# 		,AgeOnAdmission
# 		, CASE WHEN DatePart(Month, AdmissionDate) >= 4
#             THEN concat(DatePart(Year, AdmissionDate), '/', DatePart(Year, AdmissionDate) + 1)
#             ELSE concat(DatePart(Year, AdmissionDate) - 1, '/', DatePart(Year, AdmissionDate) )
#        END AS Fiscal_Year
# 
# from [SUS].[VwInpatientEpisodesPatientGeography] g
# inner join [EAT_Reporting].[dbo].[tbIpDiagnosisRelational] d  on g.EpisodeId=d.EpisodeId
# --inner join EAT_Reporting.dbo.tbInpatientEpisodes e on d.EpisodeId=e.EpisodeId
# inner join [EAT_Reporting_BSOL].[Development].[BSOL_1252_SUS_LTC_ICD10] i on i.ICD10_Code=d.DiagnosisCode  --has the different ICD10 codes for most LTCs
# where LTC_Condition='CHD' --the diagnoses
# and g.OrderInSpell='1'
# --and ReconcilliationPoint between  '201901' and '202403'
# --and ReconcilliataionPoint between '202101' and '202403'
# and left(admissionmethodcode,1)='2' --emergency admissions
# and NHSNumber is not null
#   and (OSLAUA = 'E08000025' or OSLAUA = 'E08000029')
#   group by EthnicCategoryCode
# 		,LowerLayerSuperOutputArea
# 		,AgeOnAdmission
# 		, CASE WHEN DatePart(Month, AdmissionDate) >= 4
#             THEN concat(DatePart(Year, AdmissionDate), '/', DatePart(Year, AdmissionDate) + 1)
#             ELSE concat(DatePart(Year, AdmissionDate) - 1, '/', DatePart(Year, AdmissionDate) )
#        END
# ")


IndicatorID=49
ReferenceID=93229

query <- paste0("
select IndicatorID, ReferenceID
,Trim(' ' from Ethnicity_Code) as EthnicCategoryCode
,LSOA_2021 LowerLayerSuperOutputArea
,Age as AgeOnAdmission
,Financial_Year
,sum(Numerator) as Numerator
from [EAT_Reporting_BSOL].[OF].IndicatorData
where indicatorID=49
Group by IndicatorID
,ReferenceID
,Ethnicity_Code,LSOA_2021,
Age,
Financial_Year
")


#query db for data
indicator_data <- dbGetQuery(con, query)

indicator_data<-indicator_data %>% mutate(AgeGroup_5_Code=ifelse(indicator_data$AgeOnAdmission<5,1,
                                                 ifelse(indicator_data$AgeOnAdmission>=5 & indicator_data$AgeOnAdmission<10, 2,
                                                        ifelse(indicator_data$AgeOnAdmission>=10 & indicator_data$AgeOnAdmission<15, 3,
                                                               ifelse(indicator_data$AgeOnAdmission>=15 & indicator_data$AgeOnAdmission<20, 4,
                                                                      ifelse(indicator_data$AgeOnAdmission>=20 & indicator_data$AgeOnAdmission<25, 5,
                                                                             ifelse(indicator_data$AgeOnAdmission>=25 & indicator_data$AgeOnAdmission<30,6,
                                                                                    ifelse(indicator_data$AgeOnAdmission>=30 & indicator_data$AgeOnAdmission<35,7,
                                                                                        ifelse(indicator_data$AgeOnAdmission>=35 & indicator_data$AgeOnAdmission<40, 8,
                                                                                               ifelse(indicator_data$AgeOnAdmission>=40 & indicator_data$AgeOnAdmission<45, 9,
                                                                                           ifelse(indicator_data$AgeOnAdmission>=45 & indicator_data$AgeOnAdmission<50, 10,
                                                                                                  ifelse(indicator_data$AgeOnAdmission>=50 & indicator_data$AgeOnAdmission<55, 11,
                                                                                                         ifelse(indicator_data$AgeOnAdmission>=55 & indicator_data$AgeOnAdmission<60, 12,
                                                                                                                ifelse(indicator_data$AgeOnAdmission>=60 & indicator_data$AgeOnAdmission<65, 13,
                                                                                                                       ifelse(indicator_data$AgeOnAdmission>=65 & indicator_data$AgeOnAdmission<70, 14,
                                                                                                                              ifelse(indicator_data$AgeOnAdmission>=70 & indicator_data$AgeOnAdmission<75, 15,
                                                                                                                                     ifelse(indicator_data$AgeOnAdmission>=75 & indicator_data$AgeOnAdmission<80, 16,
                                                                                                                                            ifelse(indicator_data$AgeOnAdmission>=80 & indicator_data$AgeOnAdmission<85, 17,
                                                                                                                                                    18))))))))))))))))))

indicator_data<-indicator_data %>% mutate(AgeGroup_5=ifelse(indicator_data$AgeOnAdmission<5,"Aged 4 years and under",
                                                                 ifelse(indicator_data$AgeOnAdmission>=5 & indicator_data$AgeOnAdmission<10, "Aged 5 to 9 years",
                                                                        ifelse(indicator_data$AgeOnAdmission>=10 & indicator_data$AgeOnAdmission<15,"Aged 10 to 14 years" ,
                                                                               ifelse(indicator_data$AgeOnAdmission>=15 & indicator_data$AgeOnAdmission<20, "Aged 15 to 19 years",
                                                                                      ifelse(indicator_data$AgeOnAdmission>=20 & indicator_data$AgeOnAdmission<25, "Aged 20 to 24 years",
                                                                                             ifelse(indicator_data$AgeOnAdmission>=25 & indicator_data$AgeOnAdmission<30,"Aged 25 to 29 years",
                                                                                                    ifelse(indicator_data$AgeOnAdmission>=30 & indicator_data$AgeOnAdmission<35,"Aged 30 to 34 years",
                                                                                                           ifelse(indicator_data$AgeOnAdmission>=35 & indicator_data$AgeOnAdmission<40, "Aged 35 to 39 years",
                                                                                                                  ifelse(indicator_data$AgeOnAdmission>=40 & indicator_data$AgeOnAdmission<45, "Aged 40 to 44 years",
                                                                                                                         ifelse(indicator_data$AgeOnAdmission>=45 & indicator_data$AgeOnAdmission<50, "Aged 45 to 49 years",
                                                                                                                                ifelse(indicator_data$AgeOnAdmission>=50 & indicator_data$AgeOnAdmission<55, "Aged 50 to 54 years",
                                                                                                                                       ifelse(indicator_data$AgeOnAdmission>=55 & indicator_data$AgeOnAdmission<60, "Aged 55 to 59 years",
                                                                                                                                              ifelse(indicator_data$AgeOnAdmission>=60 & indicator_data$AgeOnAdmission<65, "Aged 60 to 64 years",
                                                                                                                                                     ifelse(indicator_data$AgeOnAdmission>=65 & indicator_data$AgeOnAdmission<70, "Aged 65 to 69 years",
                                                                                                                                                            ifelse(indicator_data$AgeOnAdmission>=70 & indicator_data$AgeOnAdmission<75, "Aged 70 to 74 years",
                                                                                                                                                                   ifelse(indicator_data$AgeOnAdmission>=75 & indicator_data$AgeOnAdmission<80, "Aged 75 to 79 years",
                                                                                                                                                                          ifelse(indicator_data$AgeOnAdmission>=80 & indicator_data$AgeOnAdmission<85, "Aged 80 to 84 years",
                                                                                                                                                                                 "Aged 85 years and over"))))))))))))))))))

#is.numeric(indicator_data$AgeOnAdmission)

#######################################################################################################
#get the ONS ethnicity grouping
query <- paste0("
   
  SELECT [NHSCode]
      ,[NHSCodeDefinition]
      ,[LocalGrouping]
      ,[CensusEthnicGroup]
      ,[ONSGroup]
  FROM [EAT_Reporting_BSOL].[OF].[lkp_ethnic_categories]

")

ONS_Ethnicity_mapping<- dbGetQuery(con, query)

########################################################################################################
# Read in reference tables
#
lsoa_ward_lad_map<-read.csv("Lower_Layer_Super_Output_Area_(2021)_to_Ward_(2022)_to_LAD_(2022)_Lookup_in_England_and_Wales_v3.csv")
##
#ward_to_locality.csv
#
#nhs_ethnic_categories.csv
#
#C21_a86_e20_ward.csv
########################################################################################################

#read in lsoa ward LAD lookup
## https://geoportal.statistics.gov.uk/datasets/fc3bf6fe8ea949869af0a018205ac952_0/explore

#lsoa_ward_lad_map <- read.csv("C:/Users/Richard.Wilson/Downloads/Lower_Layer_Super_Output_Area_(2021)_to_Ward_(2022)_to_LAD_(2022)_Lookup_in_England_and_Wales_v3.csv", header=TRUE, check.names=FALSE)
#correct column names to be R friendly
names(lsoa_ward_lad_map) <- str_replace_all(names(lsoa_ward_lad_map), c(" " = "_" ,
                                                              "/" = "_",
                                                              "\\(" = ""  ,
                                                              "\\)" = "" ))
colnames(lsoa_ward_lad_map)[1] <- 'LSOA21CD'

lsoa_ward_lad_map <-  lsoa_ward_lad_map %>%  ##JA
  filter(LAD22CD == 'E08000025' | LAD22CD == 'E08000029')

#read in ward lookup
ward_locality_map <- read.csv("ward_to_locality.csv", header = TRUE, check.names = FALSE)
#correct column names to be R friendly
names(ward_locality_map) <- str_replace_all(names(ward_locality_map), c(" " = "_" ,
                                                              "/" = "_",
                                                              "\\(" = ""  ,
                                                              "\\)" = "" ))
colnames(ward_locality_map)[1] <- 'LA'
colnames(ward_locality_map)[3] <- 'WardCode'
colnames(ward_locality_map)[4] <- 'WardName'


indicator <- indicator_data %>%
  left_join(lsoa_ward_lad_map, by = c("LowerLayerSuperOutputArea" = "LSOA21CD")) %>%
  left_join(ward_locality_map, by = c("WD22NM" = "WardName")) %>%
  group_by(IndicatorID, ReferenceID,EthnicCategoryCode, Financial_Year, LAD22CD,
           WD22CD, WD22NM, Locality,AgeGroup_5,AgeGroup_5_Code) %>%  #JA
  summarise(Numerator = sum(Numerator, na.rm = TRUE)) %>%
  select(IndicatorID,ReferenceID,Numerator, EthnicCategoryCode, Financial_Year, LAD22CD,
         WD22CD, WD22NM, Locality,AgeGroup_5,AgeGroup_5_Code) %>% 
  mutate(Fiscal_Year = str_replace(Financial_Year, "-", "/20"))

#get periods so that geographies with 0 numerators will be populated
periods <- indicator %>%
  group_by(Fiscal_Year) %>%
  summarise(count = n()) %>%
  select(-count)



#create list of localities from indicator file - this should come from reference file in future
localities <- indicator %>%
  group_by(Locality) %>%
  summarise(count = n()) %>%
  filter(!is.na(Locality)) %>%
  select(-count)



#read in ethnic code translator
ethnic_codes <- read.csv("nhs_ethnic_categories.csv", header = TRUE, check.names = FALSE)
ethnic_codes <- ethnic_codes %>%
  select(NHSCode, CensusEthnicGroup, NHSCodeDefinition) %>% 
  inner_join (ONS_Ethnicity_mapping,by=c("CensusEthnicGroup"="CensusEthnicGroup")) %>% 
  select(NHSCode.x, CensusEthnicGroup, NHSCodeDefinition.x,ONSGroup) %>% 
  rename("NHSCode"=1,"NHSCodeDefinition"=3)



#read in population file for wards
#popfile_ward <- read.csv("C21_a86_e20_ward.csv", header = TRUE, check.names = FALSE)
popfile_ward <- read.csv("c21_a18_e20_s2_ward.csv", header = TRUE, check.names = FALSE)  ###########JA

#try<-read.csv("c21_a11_e20_s2_ward.csv", header = TRUE, check.names = FALSE)

#correct column names to be R friendly
names(popfile_ward) <- str_replace_all(names(popfile_ward), c(" " = "_" ,
                                                            "/" = "_",
                                                            "\\(" = ""  ,
                                                            "\\)" = "" ))


#JA: population by ward, by ethnicity, by age (for the standardisation)
#JA: need ward lad map to filter BSOL rather than filter the numerator because this removes the entries where you have
#JA: 0 numerator
lsoa_LAD<-distinct(data.frame(lsoa_ward_lad_map$WD22CD,lsoa_ward_lad_map$LAD22CD)) %>%
          rename("WD22CD"=1,"LAD22CD"=2)

##JA: getting locality by ward
localitiesWard<-distinct(data.frame(indicator$Locality,indicator$WD22CD, indicator$WD22NM)) %>%
        rename("Locality"=1,"WD22CD"=2,"WD22NM"=3)




popfile_ward <- popfile_ward %>%
  inner_join(lsoa_LAD, by = c("Electoral_wards_and_divisions_Code" = "WD22CD"))  %>%
#  filter(Age_86_categories_Code >= AgeMin & Age_86_categories_Code <= AgeMax) %>% ######JA
      filter((LAD22CD == 'E08000025' | LAD22CD== 'E08000029') )%>%
  group_by(Electoral_wards_and_divisions_Code, Electoral_wards_and_divisions,
           Ethnic_group_20_categories_Code, Ethnic_group_20_categories,
          Age_B_18_categories_Code,Age_B_18_categories ) %>%
  summarise(Observation = sum(Observation))




######################################################################################################################
#Add IMD quintiles
# using IMD package
######################################################################################################################

#get IMD score by ward
imd_england_ward <- IMD::imd_england_ward %>%
  select(ward_code, Score)

#add quintiles to ward
#JA: ward by IMD quintiles
imd_england_ward <- phe_quantile(imd_england_ward, Score, nquantiles = 5L, invert=TRUE)

imd_england_ward <- imd_england_ward %>%
  select(-Score, -nquantiles, -groupvars, -qinverted)

#add quintile to popfile
#JA: join the population with IMD (by ward) and join with the census ethnic code
popfile_ward <- popfile_ward %>%
  left_join(imd_england_ward, by = c("Electoral_wards_and_divisions_Code" = "ward_code")) %>%
  left_join(ethnic_codes, by = c("Ethnic_group_20_categories_Code" = "CensusEthnicGroup" ))

######################################################################################################################
#create population file for each year and each geography
######################################################################################################################
#ward by ethnicity,  IMD quintile JA: with the right ethnicity code, and fisical years repeated
pop_ward<- popfile_ward %>%
  group_by(Electoral_wards_and_divisions_Code,Electoral_wards_and_divisions,  Ethnic_group_20_categories_Code,
           NHSCode, NHSCodeDefinition,ONSGroup, quantile ,Age_B_18_categories_Code,Age_B_18_categories) %>%
  summarise(Denominator = sum(Observation, na.rm = TRUE))  %>%
  cross_join(periods)


######################################################################################################################
#rates for BSOL
######################################################################################################################

##JA: sort out the standard population and stick the right age groups to it-to make them 18 groups we need to join the last two
##JA:age groups
AgeGroups<-distinct(data.frame(popfile_ward$Age_B_18_categories_Code,popfile_ward$Age_B_18_categories))

Std_pop<-tibble(AgeGroups,
       c(esp2013[1:17],sum(esp2013[18:19]))) %>%
  rename("AgeBand_Code"=1,"AgeBand"=2, "Population"=3)



#overall indicator rate for BSol
#join numerator with denominator by all the variables

##age standardised rate for the different years
indicator_rate_BSol <- pop_ward %>%
  left_join(indicator, by = c("Electoral_wards_and_divisions_Code" = "WD22CD",
                          "NHSCode" = "EthnicCategoryCode",
                          "Fiscal_Year" = "Fiscal_Year",
                           "Age_B_18_categories_Code" ="AgeGroup_5_Code"

                          )) %>%
  #filter((LAD22CD == 'E08000025' | LAD22CD == 'E08000029') ) %>% JA: removed this because it removes the 0 values in the numerator and filtered denominator instead
group_by(Fiscal_Year,Age_B_18_categories_Code,Age_B_18_categories) %>%
  summarise(Numerator = sum(Numerator, na.rm = TRUE),
            Denominator = sum(Denominator)) %>%
  mutate(Gender = Demo_Gender,
         #AgeGrp = Demo_age, #JA removed
        IMD = NA,
        Ethnicity = NA,
        AggID = 'BSOL ICB',
        AggType='ICB'
  )  %>%
  group_by(AggType,AggID,
            Gender,  IMD, Ethnicity,
           Fiscal_Year)   %>%
  left_join(Std_pop, by=c("Age_B_18_categories_Code"="AgeBand_Code")) %>% #JA: Added
  phe_dsr(x=Numerator,
          n=Denominator,
          stdpop= Population,
          stdpoptype = "field",
          type = "standard",
          multiplier = 100000) %>%
  rename("IndicatorValue" = value)





#ethnicity indicator rate for BSol
##JA:issue, looks like all the unknow ethnicity (Z NHS code and -8 census code, have denominator of 0)
#m<- read.csv("c21_a18_e20_s2_ward.csv", header = TRUE, check.names = FALSE)
#n<-m[m$Observation==0,]

indicator_rate_BSol_by_ethnicity <- pop_ward %>%
  left_join(indicator, by = c("Electoral_wards_and_divisions_Code" = "WD22CD",
                             "NHSCode" = "EthnicCategoryCode",
                             "Fiscal_Year" = "Fiscal_Year",
                             "Age_B_18_categories_Code" ="AgeGroup_5_Code"
  )) %>%
#  filter((LAD22CD == 'E08000025' | LAD22CD == 'E08000029') ) %>% #JA:removed
  group_by(Fiscal_Year,ONSGroup,Age_B_18_categories_Code,Age_B_18_categories) %>% ##, NHSCode
  summarise(Numerator = sum(Numerator, na.rm = TRUE),
            Denominator = sum(Denominator)) %>%
  mutate(Numerator = ifelse((is.na(Numerator) | Denominator==0), 0, Numerator), ##make the numerator=0 if denominator=0 and then give a value to the denominator to end up with zero rate
         Denominator=ifelse(Denominator==0,1,Denominator), ###JA: put a value for denomnator=0 to avoid errors but numerator made =0 to get zero rate
        Gender = Demo_Gender,
         #AgeGrp = Demo_age,
         IMD = NA,
         Ethnicity = ONSGroup,
         AggID = 'BSOL ICB',
        AggType='ICB'
        
 ) %>%
  #filter(Denominator > 0 ) %>%  ##JA remove
  group_by(AggType,AggID,
           Gender,  IMD, Ethnicity,
           Fiscal_Year)   %>%
  left_join(Std_pop, by=c("Age_B_18_categories_Code"="AgeBand_Code")) %>% #JA: Added
  phe_dsr(x=Numerator,
          n=Denominator,
          stdpop= Population,
          stdpoptype = "field",
          type = "standard",
          multiplier = 100000) %>%
  rename("IndicatorValue" = value) %>% 
  filter(!is.na(Ethnicity))



  # phe_rate(Numerator, Denominator, type = "standard", multiplier = 100000) %>%
  # rename("IndicatorValue" = value)


#IMD indicator rate for BSol
indicator_rate_BSol_by_IMD <- pop_ward %>%
  left_join(indicator, by = c("Electoral_wards_and_divisions_Code" = "WD22CD",
                             "NHSCode" = "EthnicCategoryCode",
                             "Fiscal_Year" = "Fiscal_Year",
                             "Age_B_18_categories_Code" ="AgeGroup_5_Code"
  )) %>%
 # filter((LAD22CD == 'E08000025' | LAD22CD == 'E08000029') ) %>%
  group_by(quantile, Fiscal_Year,Age_B_18_categories_Code,Age_B_18_categories) %>%
  summarise(Numerator = sum(Numerator, na.rm = TRUE),
            Denominator = sum(Denominator)) %>%
  mutate(Numerator = ifelse((is.na(Numerator)| Denominator==0), 0, Numerator), ##JA:add denom=0
         Denominator=ifelse(Denominator==0,1,Denominator), ###JA: see above
         Gender = Demo_Gender,
        # AgeGrp = Demo_age,
         IMD = paste0('Q', quantile),
         Ethnicity = NA,
         AggID = 'BSOL ICB',
        AggType='ICB'
  ) %>%
 # filter(!is.na(quantile) & Denominator > 0) %>%
group_by(AggType,AggID, Gender, IMD, Ethnicity, Fiscal_Year) %>%
  left_join(Std_pop, by=c("Age_B_18_categories_Code"="AgeBand_Code")) %>% #JA: Added
  phe_dsr(x=Numerator,
          n=Denominator,
          stdpop= Population,
          stdpoptype = "field",
          type = "standard",
          multiplier = 100000) %>%
  rename("IndicatorValue" = value)%>% 
  filter(!is.na(IMD))




#ethnicity by IMD indicator rate for BSol
indicator_rate_BSol_by_ethnicityXIMD <- pop_ward %>%
  left_join(indicator, by= c("Electoral_wards_and_divisions_Code" = "WD22CD",
                             "NHSCode" = "EthnicCategoryCode",
                             "Fiscal_Year" = "Fiscal_Year",
                             "Age_B_18_categories_Code" ="AgeGroup_5_Code"
  )) %>%
  #filter((LAD22CD == 'E08000025' | LAD22CD == 'E08000029') ) %>%  #JA:remove
  group_by(quantile, ONSGroup, Fiscal_Year,Age_B_18_categories_Code,Age_B_18_categories) %>%
  summarise(Numerator = sum(Numerator, na.rm = TRUE),
            Denominator = sum(Denominator)) %>%
  mutate(Numerator = ifelse((is.na(Numerator)| Denominator==0), 0, Numerator), ##JA:see above
         Denominator=ifelse(Denominator==0,1,Denominator), ###JA: see above
         Gender = Demo_Gender,
         #AgeGrp = Demo_age, JA: remove
         IMD = paste0('Q', quantile),
         Ethnicity = ONSGroup,
         AggID = 'BSOL ICB',
         AggType='ICB'
  ) %>%
  #filter(!is.na(quantile) & Denominator >0) %>% JA: remove
  group_by(AggType, AggID, Gender, IMD, Ethnicity, Fiscal_Year) %>%
  left_join(Std_pop, by=c("Age_B_18_categories_Code"="AgeBand_Code")) %>% #JA: Added
  phe_dsr(x=Numerator,
          n=Denominator,
          stdpop= Population,
          stdpoptype = "field",
          type = "standard",
          multiplier = 100000) %>%
  rename("IndicatorValue" = value)%>% 
  filter(!is.na(Ethnicity))


######################################################################################################################
#rates for LA
######################################################################################################################

#overall indicator rate for local authority
indicator_rate_LA <- pop_ward %>%
  left_join(indicator, by = c("Electoral_wards_and_divisions_Code" = "WD22CD",
                             "NHSCode" = "EthnicCategoryCode",
                             "Fiscal_Year" = "Fiscal_Year",
                             "Age_B_18_categories_Code" ="AgeGroup_5_Code" #JA
  )) %>% left_join(lsoa_LAD, by=c("Electoral_wards_and_divisions_Code"="WD22CD")) %>%  ##JA:added this because I need all values for all ages even when numerator does not exist for the standardisation to work
 #!!!!!!JA: might be better joining LAD and locality to pop_ward from the begnning
  #filter((LAD22CD == 'E08000025' | LAD22CD == 'E08000029') ) %>% #JA
  group_by(LAD22CD.y, Fiscal_Year,Age_B_18_categories_Code,Age_B_18_categories) %>% ##JA:took LAD22CD.y because it is the complete one
  summarise(Numerator = sum(Numerator, na.rm = TRUE),
            Denominator = sum(Denominator)) %>%

  mutate(Numerator = ifelse((is.na(Numerator)| Denominator==0), 0, Numerator), ##JA:see above
         Denominator=ifelse(Denominator==0,1,Denominator), ###JA: see above
         Gender = Demo_Gender,
         #AgeGrp = Demo_age,
         IMD = NA,
         Ethnicity = NA,
         AggID = LAD22CD.y,
         AggType='Local Authority'
  )  %>%
 group_by(AggType,AggID, Gender, IMD, Ethnicity,  Fiscal_Year)   %>%
  left_join(Std_pop, by=c("Age_B_18_categories_Code"="AgeBand_Code")) %>% #JA: Added
  phe_dsr(x=Numerator,
          n=Denominator,
          stdpop= Population,
          stdpoptype = "field",
          type = "standard",
          multiplier = 100000) %>%
  rename("IndicatorValue" = value)



#rates for LA by ethnicity
indicator_rate_LA_by_ethnicity <- pop_ward %>%
  left_join(indicator, by = c("Electoral_wards_and_divisions_Code" = "WD22CD",
                             "NHSCode" = "EthnicCategoryCode",
                             "Fiscal_Year" = "Fiscal_Year",
                             "Age_B_18_categories_Code" ="AgeGroup_5_Code" #JA
  )) %>% left_join(lsoa_LAD, by=c("Electoral_wards_and_divisions_Code"="WD22CD")) %>%  ##JA:added
  #filter((LAD22CD == 'E08000025' | LAD22CD == 'E08000029') ) %>% ##JA:remove
  group_by(Fiscal_Year,LAD22CD.y, ONSGroup,Age_B_18_categories_Code,Age_B_18_categories) %>% #JA added age
  summarise(Numerator = sum(Numerator, na.rm = TRUE),
            Denominator = sum(Denominator)) %>%
  mutate(Numerator = ifelse((is.na(Numerator)| Denominator==0), 0, Numerator), ##JA:see above
         Denominator=ifelse(Denominator==0,1,Denominator), ###JA: see above
         Gender = Demo_Gender,
         #AgeGrp = Demo_age,
         IMD = NA,
         Ethnicity = ONSGroup,
         AggID = LAD22CD.y,
         AggType='Local Authority'
  )  %>%
 # filter(Denominator > 0 ) %>% ##JA
  group_by(AggType,AggID, Gender,IMD, Ethnicity, Fiscal_Year) %>%
  left_join(Std_pop, by=c("Age_B_18_categories_Code"="AgeBand_Code")) %>% #JA: Added
  phe_dsr(x=Numerator,
          n=Denominator,
          stdpop= Population,
          stdpoptype = "field",
          type = "standard",
          multiplier = 100000) %>%
  rename("IndicatorValue" = value)%>% 
  filter(!is.na(Ethnicity))


#IMD indicator rate for local authority
  indicator_rate_LA_by_IMD <- pop_ward %>%
    left_join(indicator, by = c("Electoral_wards_and_divisions_Code" = "WD22CD",
                               "NHSCode" = "EthnicCategoryCode",
                               "Fiscal_Year" = "Fiscal_Year",
                               "Age_B_18_categories_Code" ="AgeGroup_5_Code" #JA
    )) %>% left_join(lsoa_LAD, by=c("Electoral_wards_and_divisions_Code"="WD22CD")) %>%  ##JA:added
    #filter((LAD22CD == 'E08000025' | LAD22CD == 'E08000029') ) %>% ##JA:remove
    group_by(LAD22CD.y,quantile, Fiscal_Year,Age_B_18_categories_Code,Age_B_18_categories) %>%
    summarise(Numerator = sum(Numerator, na.rm = TRUE),
              Denominator = sum(Denominator)) %>%
  mutate(Numerator = ifelse((is.na(Numerator)| Denominator==0), 0, Numerator), ##JA:see above
         Denominator=ifelse(Denominator==0,1,Denominator), ###JA: see above
         Gender = Demo_Gender,
        # AgeGrp = Demo_age,
         IMD = paste0('Q', quantile),
         Ethnicity = NA,
         AggID = LAD22CD.y,
        AggType='Local Authority' ) %>%
    group_by(AggType,AggID, Gender,IMD, Ethnicity, Fiscal_Year) %>%
    left_join(Std_pop, by=c("Age_B_18_categories_Code"="AgeBand_Code")) %>% #JA: Added
    phe_dsr(x=Numerator,
            n=Denominator,
            stdpop= Population,
            stdpoptype = "field",
            type = "standard",
            multiplier = 100000) %>%
    rename("IndicatorValue" = value)

######################################################################################################################
#rates for localities
######################################################################################################################

#Rates for localities
  indicator_rate_Locality <- pop_ward %>%
    left_join(indicator, by = c("Electoral_wards_and_divisions_Code" = "WD22CD",
                               "NHSCode" = "EthnicCategoryCode",
                               "Fiscal_Year" = "Fiscal_Year",
                               "Age_B_18_categories_Code" ="AgeGroup_5_Code" #JA

    )) %>%left_join(localitiesWard, by=c("Electoral_wards_and_divisions_Code"="WD22CD")) %>%  ##JA:added
    #JA:better join locality to pop_ward from the begnning!!
   # filter((LAD22CD == 'E08000025' | LAD22CD == 'E08000029') ) %>%
    group_by(Locality.y, Fiscal_Year,Age_B_18_categories_Code,Age_B_18_categories) %>%
    summarise(Numerator = sum(Numerator, na.rm = TRUE),
              Denominator = sum(Denominator)) %>%
    mutate(Numerator = ifelse((is.na(Numerator)| Denominator==0), 0, Numerator), ##JA:see above
           Denominator=ifelse(Denominator==0,1,Denominator), ###JA: see above
           Gender = Demo_Gender,
          # AgeGrp = Demo_age,
           IMD = NA,
           Ethnicity = NA,
           AggID = Locality.y,
          AggType='Locality (resident)'
    )  %>%
    group_by(AggType,AggID, Gender,  IMD, Ethnicity, Fiscal_Year)   %>%
    left_join(Std_pop, by=c("Age_B_18_categories_Code"="AgeBand_Code")) %>% #JA: Added
    phe_dsr(x=Numerator,
            n=Denominator,
            stdpop= Population,
            stdpoptype = "field",
            type = "standard",
            multiplier = 100000) %>%
    rename("IndicatorValue" = value)

#rates for Locality by ethnicity
indicator_rate_Locality_by_ethnicity <- pop_ward %>%
    left_join(indicator, by= c("Electoral_wards_and_divisions_Code" = "WD22CD",
                               "NHSCode" = "EthnicCategoryCode",
                               "Fiscal_Year" = "Fiscal_Year",
                               "Age_B_18_categories_Code" ="AgeGroup_5_Code" #JA
    )) %>%left_join(localitiesWard, by=c("Electoral_wards_and_divisions_Code"="WD22CD")) %>%  ##JA:added
  #JA:better join locality to pop_ward from the begnning!!
    group_by(Fiscal_Year, Locality.y, ONSGroup,Age_B_18_categories_Code,Age_B_18_categories) %>%
    summarise(Numerator = sum(Numerator, na.rm = TRUE),
              Denominator = sum(Denominator)) %>%
  mutate(Numerator = ifelse((is.na(Numerator)| Denominator==0), 0, Numerator), ##JA:see above
         Denominator=ifelse(Denominator==0,1,Denominator), ###JA: see above
         Gender = Demo_Gender,
         # AgeGrp = Demo_age,
         IMD = NA,
           Ethnicity = ONSGroup,
           AggID = Locality.y,
         AggType='Locality (resident)'
    )  %>%
   # filter(Denominator > 0 ) %>% #JA
    group_by(AggType,AggID, Gender, IMD, Ethnicity,  Fiscal_Year) %>%
  left_join(Std_pop, by=c("Age_B_18_categories_Code"="AgeBand_Code")) %>% #JA: Added
  phe_dsr(x=Numerator,
          n=Denominator,
          stdpop= Population,
          stdpoptype = "field",
          type = "standard",
          multiplier = 100000) %>%
  rename("IndicatorValue" = value)%>% 
  filter(!is.na(Ethnicity))



#IMD indicator rate for Locality
  indicator_rate_Locality_by_IMD <- pop_ward %>%
    left_join(indicator, by= c("Electoral_wards_and_divisions_Code" = "WD22CD",
                               "NHSCode" = "EthnicCategoryCode",
                               "Fiscal_Year" = "Fiscal_Year",
                               "Age_B_18_categories_Code" ="AgeGroup_5_Code" #JA
    )) %>%left_join(localitiesWard, by=c("Electoral_wards_and_divisions_Code"="WD22CD")) %>%  ##JA:added
    #JA:better join locality to pop_ward from the begnning!!
    group_by(Fiscal_Year, Locality.y, quantile,Age_B_18_categories_Code,Age_B_18_categories) %>%
    summarise(Numerator = sum(Numerator, na.rm = TRUE),
              Denominator = sum(Denominator)) %>%
    mutate(Numerator = ifelse((is.na(Numerator)| Denominator==0), 0, Numerator), ##JA:see above
           Denominator=ifelse(Denominator==0,1,Denominator), ###JA: see above
           Gender = Demo_Gender,
           # AgeGrp = Demo_age,
           IMD = paste0('Q',quantile),
           Ethnicity = NA,
           AggID = Locality.y,
           AggType='Locality (resident)'
    )  %>%
    # filter(Denominator > 0 ) %>% #JA
    group_by(AggType, AggID, Gender, IMD, Ethnicity,  Fiscal_Year) %>%
    left_join(Std_pop, by=c("Age_B_18_categories_Code"="AgeBand_Code")) %>% #JA: Added
    phe_dsr(x=Numerator,
            n=Denominator,
            stdpop= Population,
            stdpoptype = "field",
            type = "standard",
            multiplier = 100000) %>%
    rename("IndicatorValue" = value)





######################################################################################################################
#rates for wards
######################################################################################################################
#overall
indicator_rate_ward <- pop_ward %>%
    left_join(indicator, by = c("Electoral_wards_and_divisions_Code" = "WD22CD",
                               "NHSCode" = "EthnicCategoryCode",
                               "Fiscal_Year" = "Fiscal_Year",
                               "Age_B_18_categories_Code" ="AgeGroup_5_Code" #JA
    )) %>%left_join(localitiesWard, by=c("Electoral_wards_and_divisions_Code"="WD22CD")) %>%  ##JA:added
    #JA:better join locality to pop_ward from the begnning!!
   ## filter((LAD22CD == 'E08000025' | LAD22CD == 'E08000029') ) %>%
  group_by(WD22NM.y,  Fiscal_Year,Age_B_18_categories_Code,Age_B_18_categories) %>%
  summarise(Numerator = sum(Numerator, na.rm = TRUE),
            Denominator = sum(Denominator, na.rm = TRUE))  %>%
  mutate(Numerator = ifelse((is.na(Numerator)| Denominator==0), 0, Numerator), ##JA:see above
         Denominator=ifelse(Denominator==0,1,Denominator), ###JA: see above
         Gender = Demo_Gender,
        # AgeGrp = Demo_age,
         IMD = NA,
         Ethnicity = NA,
         AggID = WD22NM.y,
        AggType='Ward') %>%
 # filter(Denominator > 0 ) %>%
    group_by(AggType,AggID, Gender, IMD, Ethnicity,  Fiscal_Year) %>%
    left_join(Std_pop, by=c("Age_B_18_categories_Code"="AgeBand_Code")) %>% #JA: Added
    phe_dsr(x=Numerator,
            n=Denominator,
            stdpop= Population,
            stdpoptype = "field",
            type = "standard",
            multiplier = 100000) %>%
    rename("IndicatorValue" = value)


# ######################################################################################################################
# #3 and 5 year rates for ward
# ######################################################################################################################
# ####JA: not sorted yet!! I think we can just add dates to the function and run it for 3 or 5 years
#   ##JA: should not calculate the sum of the denominator for 3 years. either take same denominator or average over 3 years!!!!
# #3 year
# indicator_3yr_ward  <- pop_ward %>%
#     left_join(indicator, by = c("Electoral_wards_and_divisions_Code" = "WD22CD",
#                                "NHSCode" = "EthnicCategoryCode",
#                                "Fiscal_Year" = "Fiscal_Year"    )) %>%
#     mutate(three_years = "NA")
# 
#   indicator_3yrrate_ward <- indicator_3yr_ward[FALSE,]
# 
#   for(year in periods){
#     husk <- indicator_3yrrate_ward[FALSE,]
# 
#     husk <-  indicator_3yr_ward %>%
#         filter(as.integer(substr(Fiscal_Year,1,4)) >= as.integer(substr(year,1,4)) &
#                as.integer(substr(Fiscal_Year,1,4)) <= as.integer(substr(year,1,4))+2) %>%
#          mutate(three_years = paste0(as.integer(substr(Fiscal_Year,1,4)),'/',  as.integer(substr(year,1,4))+2))
# 
#     indicator_3yrrate_ward <- rbind(indicator_3yrrate_ward,husk)
#     }
# 
#  indicator_3yrrate_ward <-   indicator_3yrrate_ward %>%
#    group_by(Electoral_wards_and_divisions,  three_years) %>%
#    summarise(Numerator = sum(Numerator, na.rm = TRUE),
#              Denominator = sum(Denominator, na.rm = TRUE))  %>%  ###JA: should not sum denominator!!!!
#     mutate(Numerator = ifelse(is.na(Numerator), 0, Numerator),
#          Gender = Demo_Gender,
#          AgeGrp = Demo_age,
#          IMD = NA,
#          Ethnicity = NA,
#          AggID = Electoral_wards_and_divisions
#   ) %>%
#   filter(Denominator > 0 & !is.na(three_years) ) %>%
#   group_by(AggID, Gender, AgeGrp, IMD, Ethnicity,  three_years) %>%
#   phe_rate(Numerator, Denominator, type = "standard", multiplier = 100000) %>%
#   rename("IndicatorValue" = value) %>%
#   rename("Fiscal_Year" = three_years)
# 
# #5 year
# 
#  indicator_5yr_ward  <- pop_ward %>%
#    left_join(indicator, by = c("Electoral_wards_and_divisions_Code" = "WD22CD",
#                                "NHSCode" = "EthnicCategoryCode",
#                                "Fiscal_Year" = "Fiscal_Year"    )) %>%
#    mutate(five_years = "NA")
# 
#  indicator_5yrrate_ward <- indicator_3yr_ward[FALSE,]
# 
#  for(year in periods){
#    husk <- indicator_5yrrate_ward[FALSE,]
# 
#    husk <-  indicator_5yr_ward %>%
#      filter(as.integer(substr(Fiscal_Year,1,4)) >= as.integer(substr(year,1,4)) &
#               as.integer(substr(Fiscal_Year,1,4)) <= as.integer(substr(year,1,4))+4) %>%
#      mutate(three_years = paste0(as.integer(substr(Fiscal_Year,1,4)),'/',  as.integer(substr(year,1,4))+4))
# 
#    indicator_5yrrate_ward <- rbind(indicator_5yrrate_ward,husk)
#  }
# 
#  indicator_5yrrate_ward <-   indicator_5yrrate_ward %>%
#    group_by(Electoral_wards_and_divisions,  five_years) %>%
#    summarise(Numerator = sum(Numerator, na.rm = TRUE),
#              Denominator = sum(Denominator, na.rm = TRUE))  %>%
#    mutate(Numerator = ifelse(is.na(Numerator), 0, Numerator),
#           Gender = Demo_Gender,
#           AgeGrp = Demo_age,
#           IMD = NA,
#           Ethnicity = NA,
#           AggID = Electoral_wards_and_divisions
#    ) %>%
#    filter(Denominator > 0 & !is.na(five_years) ) %>%
#    group_by(AggID, Gender, AgeGrp, IMD, Ethnicity,  five_years) %>%
#    phe_rate(Numerator, Denominator, type = "standard", multiplier = 100000) %>%
#    rename("IndicatorValue" = value) %>%
#    rename("Fiscal_Year" = five_years)
######################################################################################################################
#output
######################################################################################################################

#bind into one
indicator_all_output <-cbind(IndicatorID=IndicatorID,
                             ReferenceID=ReferenceID,
                             rbind(indicator_rate_ward,
                      ##indicator_5yrrate_ward,
                      ##indicator_3yrrate_ward,
                   indicator_rate_LA,
                   indicator_rate_Locality,
                   indicator_rate_BSol,
                   indicator_rate_LA_by_ethnicity,
                   indicator_rate_Locality_by_ethnicity,
                   indicator_rate_BSol_by_ethnicity,
                   indicator_rate_LA_by_IMD,
                  indicator_rate_Locality_by_IMD,
                   indicator_rate_BSol_by_IMD,
                   indicator_rate_BSol_by_ethnicityXIMD)) %>% 
                 mutate( InsertDate = today(),
                  IndicatorStartDate = as.Date(ifelse(is.na(Fiscal_Year), NA, paste0(substring(Fiscal_Year, 1, 4), '-04-01'))),
                  IndicatorEndDate = as.Date(ifelse(is.na(Fiscal_Year), NA, paste0('20', substring(Fiscal_Year, 8, 9), '-03-31'))),
                  StatusID = as.integer(1), # current
                  DataQualityID = as.integer(1))
 
 #write.csv(indicator_all_output, "CVD_indicator93229_ONS.csv")
 
 dbWriteTable(
   con,
   Id(schema="OF",table="BSOL_0033_OF_93229_CHD_StandardisedRate"),
   indicator_all_output,
   overwrite=TRUE,
   field.types=c(
     IndicatorID="float",
    InsertDate="datetime",
    AggType="nvarchar(50)",
    AggID="nvarchar(200)",
    Gender="nvarchar(20)",
    IMD="nvarchar(20)",
    Ethnicity="nvarchar(200)",
    Fiscal_Year="nvarchar(50)",
   
    IndicatorValue="float",
    lowercl="float",
    uppercl="float",
    total_count="float",
    total_pop="float",
   #  Numerator="float",
    #Denominator="float",
    StatusID="smallint",
    DataQualityID="smallint",
    IndicatorStartDate="datetime",
    IndicatorEndDate="datetime"
     
     
   )
 )

# #set metadata
# indicator_out <- indicator_all_output %>%
#   filter(Fiscal_Year!= '2013/2014') %>%
#   mutate(IndicatorID = 90810,
#          valueID = 1,
#          insertdate = today(),
#          IndicatorStartDate = ifelse(is.na(Fiscal_Year), NA, paste0(substring(Fiscal_Year, 1, 4),'-04-01')),
#          IndicatorEndDate = ifelse(is.na(Fiscal_Year), NA, paste0('20', substring(Fiscal_Year, 8, 9),'-03-31')),
#          StatusID = 1 #current
#          ) %>%
#   left_join(ethnic_codes, by = c ("Ethnicity"  = "NHSCode")) %>%
#   left_join(demo_table, by = c('Gender' = 'Gender'
#                                ,'AgeGrp' = 'AgeGrp'
#                                ,'IMD' = 'IMD'
#                                ,'NHSCodeDefinition' = 'Ethnicity'
#   )) %>%
#   ungroup() %>%
#   select(AggID, Numerator, Denominator, IndicatorValue, lowercl, uppercl, IndicatorID, valueID, IndicatorStartDate,
#          IndicatorEndDate, DemographicID)
# 
# write.csv(indicator_out, 'asthma_test_full.csv')

#try to write to warehouse
#dbWriteTable(con,"OF.IndicatorValue", indicator_out, append = TRUE)

#sqlAppendTable(con, "OF.IndicatorValue", indicator_out, row.names = NA)
