library(redcapAPI)
library(dplyr) # group_by and mutate functions
library(tidyr) # pivot_wider function
library(lubridate)
library(zoo)
library(googlesheets4)
library(stringr)
#library('plyr')

#those generated for may that are actually pending for June
#Duplicates
#next visit
#m18 verification and run
#Call details is not copied to house (combine phone and details)
#breakpoints
#work on the next visit date for those returning row 0(use if>0)


#eliminate duplicates
#change Kwabs options in the approval section of the from pending for some
#visit "disabled_absent_in_eligible_or_completed" again


source("../icaria_functions.R")
source("../icaria_project_tokens.R")
source("../google_links.R")
#source("hf_names.R")
source("../contacts.R")

gs4_auth()
1

# dr fombah,  mrs bundu sae,  post death/completed, death date discrep,  github, microtik, sae without death
surveillance_threshold_days <- 47

period_under_consideration <- "2024-05"

months_with_31 <- c('01','03','05','07','08','10','12')
months_with_30 <- c('04','06','09','11')

start_date <- paste0(period_under_consideration,"-01")

if(substr(period_under_consideration,6,7) %in% months_with_31){
  end_date <- paste0(period_under_consideration,"-31")
} else if(substr(period_under_consideration,6,7) %in% months_with_30){
  end_date <- paste0(period_under_consideration,"-30")
} else{
  if((as.numeric(substr(period_under_consideration,1,4)) %% 4) == 0){
    end_date <- paste0(period_under_consideration,"-29")
  }else if((as.numeric(substr(period_under_consideration,1,4)) %% 4) != 0){
    end_date <- paste0(period_under_consideration,"-28")
  }
}



start_date <- as.Date(start_date)
end_date <- as.Date(end_date)

#print(start_date)
#print(end_date)

period_link_column <- paste(start_date, "to", end_date)

fu_links <- range_read( fu_tracker_links_sheet,
                                  sheet = 'fu links',
                                  range = NULL,
                                  col_names = TRUE,
                                  col_types = NULL,
                                  na = "",
                                  trim_ws = TRUE,
                                  skip = 0,
                                  #n_max = Inf,
                                  #guess_max = min(1000, n_max),
                                  .name_repair = "unique"
)

link_frame <- fu_links[, c('HF', period_link_column)]

#Evidences

evidence_related_links <- range_read( fu_tracker_links_sheet,
                        sheet = 'evidence_related',
                        range = NULL,
                        col_names = TRUE,
                        col_types = NULL,
                        na = "",
                        trim_ws = TRUE,
                        skip = 0,
                        #n_max = Inf,
                        #guess_max = min(1000, n_max),
                        .name_repair = "unique"
)



fu_sheets <- c('Surveillance_Calls',
               'Surveillance_House',
               'MRV2',
               'M18'
              )

to_sheet <- function(data, tab, event_name, sheet_identifier, contact_meth = NULL){
  data_to_push <- filter(data, redcap_event_name == event_name & !(is.na(sheet_identifier)))
  if(tab == "Surveillance_Calls"){
    data_to_push <- filter(data_to_push, (contact_meth == "call" & !is.na(contact_meth)))
  }
  
  data_to_push$Assignee <- ""
  data_to_push$Assignation_date <- ""
  data_to_push$Assignation_date <- ""
  
  range_write(link_frame[link_frame$HF == faclity, period_link_column],
              data_to_push,
              sheet = tab,
              range = tab,
              col_names = T,
              reformat = F
  )
}

#picture_verification <- function(redcap_data, picture_data)


#start_date <- as_date(start_date, format = "%d-%m-%Y %H:%M:%S")
#end_date <- as_datetime(end_date, format = "%d-%m-%Y %H:%M:%S")


first_home_hhfu <- data.frame()
month18_home_visits <- data.frame()
new_surveillance_home_visits <- data.frame()
mrv2_related_fu <- data.frame()

hfs <- c("HF01","HF02","HF03","HF04","HF05","HF06","HF08","HF10","HF11","HF12","HF13","HF15","HF16","HF17")

# PART2: Export and bind data from all health facilities
identifiers <- c('record_id',
                 'child_dob',
                 'study_number'
                 )

alarm_fields <- c('child_fu_status')

phone_contact_fields <- c('phone_1',
                          'phone_2')
               
obsolete_fields <- c('comp_contact_type',
                     'comp_caretaker_phone',
                     'comp_date',
                     'comp_interviewer_id',
                     'a1m_contact_type',
                     'a1m_child_status',
                     'a1m_date',
                     'a1m_interviewer_id',
                     'ae_date',
                     'ae_interviewer_id'
                     )

intervention_fields <- c('int_azi',
                         'int_sp',
                         'int_vacc_rtss1',
                         'int_vacc_rtss1_date',
                         'int_vacc_rtss2',
                         'int_vacc_rtss2_date',
                         'int_vacc_rtss3',
                         'int_vacc_rtss3_date',
                         'int_vacc_rtss4',
                         'int_vacc_rtss4_date',
                         'int_next_visit',
                         'int_date',
                         'int_interviewer_id'
                         )

extra_intervention_fields <- c('int_child_weight',
                              'int_sp_dose',
                              'int_sp_vomit_extra_dose',
                              'int_random_letter',
                              'int_azi_prep_time',
                              'int_azi_dose',
                              'int_azi_admin_time',
                              'int_azi_vomit_extra_dose',
                              'int_interviewer_id'
                              )

rtss_fields <- c('rtss_date',
                 'rtss_interviewer_id',
                 'rtss_vacc_rtss1',
                 'rtss_vacc_rtss1_date',
                 'rtss_vacc_rtss2',
                 'rtss_vacc_rtss2_date',
                 'rtss_vacc_rtss3',
                 'rtss_vacc_rtss3_date',
                 'rtss_vacc_rtss4',
                 'rtss_vacc_rtss4_date')


mortality_surv_fields <- c('ms_date_contact',
                           'ms_contact_caretaker',
                           'ms_respondent_form',
                           'ms_respondent_other',
                           'ms_status_child',
                           'ms_date',
                           'ms_interviewer_id'
                           )

sae_fields <- c('sae_awareness_date',
                'sae_hosp_admin_date',
                'sae_hosp_disch_date',
                'sae_date',
                'sae_interviewer_id')
  
unscheduled_fields <- c('unsch_visit_date',
                        'unsch_date',
                        'unsch_interviewer_id'
                        )

icaria_cohort_fields <- c('ch_his_date',
                          'ch_his_interviewer_id',
                          'ch_rdt_date')

migration_fields <- c('mig_date',
                      'mig_interviewer_id')

terminal_fields <- c('death_date',
                     'death_interviewer_id',
                     'wdrawal_date',
                     'wdrawal_interviewer_id')

hhfu <- c('fu_type',
          'phone_child_status',
          'phone_success',
          'hh_child_seen',
          'reachable_status',
          'hh_why_not_child_seen',
          'hh_date',
          'hh_interviewer_id')

death_fields <- c('death_reported_date',
                 'death_date')

withdrawal_fields <- c('wdrawal_date')

azivac_fields <- c('azivac_blood_sample',
                   'azivac_age_at_mrv1',
                   'azivac_date')

all_fields <- c(identifiers,
                alarm_fields,
                phone_contact_fields,
                obsolete_fields,
                intervention_fields,
                extra_intervention_fields,
                rtss_fields,
                mortality_surv_fields,
                sae_fields,
                unscheduled_fields,
                icaria_cohort_fields,
                migration_fields,
                terminal_fields,
                hhfu,
                death_fields,
                withdrawal_fields,
                azivac_fields)


event_identifiers <- list(EPI = c('int_date', 'int_interviewer_id'),
                          rtss = c('rtss_date', 'rtss_interviewer_id'),
                          #rtss = c('rtss_vacc_rtss1_date', 'rtss_interviewer_id'),
                          #rtss = c('rtss_vacc_rtss2_date', 'rtss_interviewer_id'),
                          #rtss = c('rtss_vacc_rtss3_date', 'rtss_interviewer_id'),
                          #rtss = c('rtss_vacc_rtss4_date', 'rtss_interviewer_id'),
                          old_M_surv = c('a1m_date', 'a1m_interviewer_id'),
                          hhfu = c('hh_date', 'hh_interviewer_id'),
                          #AE = c('ae_date','ae_interviewer_id'),
                          SAE = c('sae_hosp_admin_date', 'sae_interviewer_id'),
                          #SAE = c('sae_hosp_disch_date', 'sae_interviewer_id'),
                          M_Surv = c('ms_date_contact', 'ms_interviewer_id'),
                          Unsch = c('unsch_date', 'unsch_interviewer_id'),
                          migration = c('mig_date', 'mig_interviewer_id'),
                          non_comp = c('comp_date', 'comp_interviewer_id'),
                          Cohort = c('ch_his_date', 'ch_his_interviewer_id'),
                          death = c('death_date', 'death_interviewer_id'),
                          wdrawal = c('wdrawal_date', 'wdrawal_interviewer_id')
)


epi_event_identifiers <- list(epipenta1_v0_recru_arm_1 = 'Penta1',
                        epipenta2_v1_iptis_arm_1 = 'Penta2',
                        epipenta3_v2_iptis_arm_1 = 'Penta3',
                        epivita_v3_iptisp3_arm_1 = 'VitA1',
                        #epirtss_m7_visit_arm_1 = 'MalVacc2',
                        #epirtss_m8_visit_arm_1 = 'MalVacc3',
                        epimvr1_v4_iptisp4_arm_1 = 'MRV1',
                        epivita_v5_iptisp5_arm_1 = 'VitA2',
                        epimvr2_v6_iptisp6_arm_1 = 'MRV2')

household_fu_identifiers <- list(hhafter_1st_dose_o_arm_1 = 'hhfu_after_1st_dose',
                                 hhat_18th_month_of_arm_1 = 'M18'
                                )

activity_order <- c("M_Surv",
                    "death",
                    "wdrawal",
                    "M18",                 
                    "MRV2",
                    "VitA2",
                    "MRV1",
                    "VitA1",
                    "rtss",
                    "MalVacc1",
                    "MalVacc2",
                    "Penta3",
                    "Penta2",
                    "Penta1",
                    "SAE",
                    "Unsch",
                    "mrv2_pending_alarm",   #Will be defined later
                    "end_of_fu_pending_alarm",  #Will be defined later
                    "migration",
                    "old_M_surv",
                    "hhfu_after_1st_dose",
                    "AE",
                    "Cohort",
                    "non_comp")

my.events <- c('epipenta1_v0_recru_arm_1', # Recruitment - AZi 1st dose
               'epipenta2_v1_iptis_arm_1',
               'epipenta3_v2_iptis_arm_1',
               'epivita_v3_iptisp3_arm_1',
               'epirtss_m7_visit_arm_1',
               'epirtss_m8_visit_arm_1',
               'epimvr1_v4_iptisp4_arm_1',
               'epivita_v5_iptisp5_arm_1',
               'epimvr2_v6_iptisp6_arm_1',
               'rtss_arm_1',
               'hhafter_1st_dose_o_arm_1',
               'adverse_events_arm_1',
               'hhat_18th_month_of_arm_1',
               'out_of_schedule_arm_1',
               'end_of_fu_arm_1'
               ) 
#_______________________________________________________________________________
admin_data <- data.frame()
data <- data.frame()
for (hf in names(kRedcapTokens)) {
  print(paste("Extracting data from", hf))
  
  rcon <- redcapConnection(kRedcapAPIURL, kRedcapTokens[[hf]])
  hf.data <- exportRecords(
    rcon,
    factors            = F,
    labels             = F,
    fields             = all_fields,
    events             = my.events,
    form_complete_auto = F
  )
  hf.data$pk <- paste(hf, hf.data$record_id, sep = "_")
  
  if ((substr(hf, 1, 2)  == "HF") & (substr(hf, 5, 5) == ".")) {
    hf <- substr(hf, 1, 4)
  } else {
    hf <-  hf
  }
  hf.data$hf <- hf
  data <- rbind(data, hf.data)
}

  

# ________Isolating study numbers to identify records in the various catgo --------

filter <- !is.na(data$study_number)
study_numbers <- data[filter, c('pk', "study_number", 'child_dob')]

recruitment_data <- filter(data, !is.na(study_number))
recruitment_data$phone_contact <- paste(recruitment_data$phone_1,
                                        recruitment_data$phone_2,
                                        sep = "_")

recruitment_data$bday_15_month <- ymd(recruitment_data$child_dob) %m+% months(15)
recruitment_data$bday_16_month <- ymd(recruitment_data$child_dob) %m+% months(16)  #for fu purposes

recruitment_data$bday_18_month <- ymd(recruitment_data$child_dob) %m+% months(18)

participants_contacts <- recruitment_data[, c('study_number',
                                              'phone_contact')]


field_data <- select(data, -c("study_number", 'child_dob'))





#Exclude migrated participants*****

#____________Field Data_____________________________________



#_____________Putting all event identifiers in 1 field________________________
field_data$ms_date_contact <- as.Date(substr(field_data$ms_date_contact, 1, 10), tz = "UTC")  #specially coding this line for msdate_contact's sake

field_data$event_date <- as.Date(NA, tz = "UTC")
field_data$interviewer <- NA
field_data$instrument <- NA
for(event_identifier in names(event_identifiers)){
  #field_data[[event_identifier]] <- (field_data[[event_identifier]])
  field_data[!is.na(field_data[event_identifiers[[event_identifier]][1]]), "event_date"] <- field_data[!is.na(field_data[event_identifiers[[event_identifier]][1]]), event_identifiers[[event_identifier]][1]]
  field_data[!is.na(field_data[event_identifiers[[event_identifier]][1]]), "instrument"] <- event_identifier
  field_data[!is.na(field_data[event_identifiers[[event_identifier]][2]]), "interviewer"] <- field_data[!is.na(field_data[event_identifiers[[event_identifier]][2]]), event_identifiers[[event_identifier]][2]]
}

for(epi_event_identifier in names(epi_event_identifiers)){
  #field_data[[event_identifier]] <- (field_data[[event_identifier]])
  field_data[field_data$redcap_event_name == epi_event_identifier, "instrument"] <- epi_event_identifiers[[epi_event_identifier]]
  #field_data[!is.na(field_data[event_identifiers[[event_identifier]][1]]), "instrument"] <- event_identifier
  #field_data[!is.na(field_data[event_identifiers[[event_identifier]][2]]), "interviewer"] <- field_data[!is.na(field_data[event_identifiers[[event_identifier]][2]]), event_identifiers[[event_identifier]][2]]
}

for(hhfu_identifier in names(household_fu_identifiers)){
  field_data[field_data$redcap_event_name == hhfu_identifier, "instrument"] <- household_fu_identifiers[[hhfu_identifier]]
}


field_data_complete <- field_data[!is.na(field_data$event_date), ]

#Age at event

errors_to_check <- field_data[is.na(field_data$event_date), ]  #possible clinical history to delete and incomplete without data

field_data_complete$event_date <- as.Date(field_data_complete$event_date)



field_data_identified <- merge(field_data_complete, study_numbers)



field_data_identified$event_age <- interval(field_data_identified$child_dob, field_data_identified$event_date)  %/% months(1)

field_data_identified$event_age_in_days <- interval(field_data_identified$child_dob, field_data_identified$event_date)  %/% days(1)

field_data_identified$event_pk <- paste(field_data_identified$study_number,
                                        field_data_identified$event_date,
                                        sep = "_")

#period_field_data_identified <- filter(field_data_identified, (event_date >= start_date & event_date <= end_date))



# DETERMINING DEAD, WITHDRAWN PARTICIPANTS before period--------------------------------

dead_participants <- merge(filter(field_data, (!is.na(death_date) & death_date < start_date)), study_numbers)

withdrawn_participants <- merge(filter(field_data, (!is.na(wdrawal_date) & wdrawal_date < start_date)), study_numbers)

completed_participants <- filter(field_data_identified, (!is.na(hh_date) &
                                                           hh_date < start_date &
                                                           instrument == 'M18'& event_age >= 18)) # before period

completed_count <- length(unique(completed_participants$study_number))

wrong_m18 <- filter(field_data_identified, (!is.na(hh_date) & instrument == 'M18'& event_age < 18))

#incomplete_count <- length(unique(wrong_m18$study_number))

wrong_m18_participants <- filter(wrong_m18, !(study_number %in% completed_participants$study_number))
wrong_m18_participants <- wrong_m18_participants[, c('hf', "study_number")]
write.csv(wrong_m18_participants, 'wrong_m18_participants.csv', row.names = F)

exited_particiants <- rbind(dead_participants[, c('hf', 'study_number')],
                            withdrawn_participants[, c('hf', 'study_number')],
                            completed_participants[, c('hf', 'study_number')]
)

#remove these from the list of data before period
field_data_identified_exited_removed <- filter(field_data_identified, !(study_number %in% exited_particiants$study_number))
# ||||||||||||||||||||||||||||||||| ---------------------------------------

# TIMELINES ---------------------------------------------------------------

#getting the latest data before period under consideration for ACTIVE PARTICIPANTS
#field_data_identified_exited_removed

field_data_identified_before_period <- filter(field_data_identified_exited_removed, event_date < start_date)
field_data_identified_before_period_grouped <- group_by(field_data_identified_before_period, study_number)
field_data_identified_before_period_latest <- filter(field_data_identified_before_period_grouped, event_date == max(event_date, na.rm = T))

# GETTING ELIGIBLE PARTICIPANTS BASED ON: ---------------------------------
  #1  For surveillance: based on the surveillance threshold date
  # For M15 and M18: based on the number of children that have turned 15 month and 18 months respectively


if(today() > end_date){
  #use end_date for calculation
  fu_reference_date <- end_date
}else{
  #use today()
  fu_reference_date <- today()
}

    #ELIGIBLE SURVEILLANCE, MRV2 AND M18
field_data_identified_before_period_latest$surveillance_days_elapsed <- interval(field_data_identified_before_period_latest$event_date, fu_reference_date)  %/% days(1)
eligible_surveillance_period_hitherto <- filter(field_data_identified_before_period_latest, surveillance_days_elapsed >=surveillance_threshold_days)
eligible_surveillance_period_hitherto$surv_eligible_date <- eligible_surveillance_period_hitherto$event_date + surveillance_threshold_days
#eligible_surveillance_period_hitherto <- eligible_surveillance_period_hitherto[, c('hf',
 #                                                                                  'study_number',
  #                                                                                 'surv_eligible_date')]

eligible_period_mrv2_hitherto <- filter(recruitment_data, bday_16_month <= fu_reference_date)  #&bday_16_month >= start_date #for fu purposes

eligible_period_end_of_fu_hitherto <- filter(recruitment_data, bday_18_month <= fu_reference_date) #&bday_18_month >= start_date

#before_period_mrv2_due <- filter(recruitment_data, bday_16_month < start_date)  #use to validate in case on entries before period

#before_period_end_of_fu_due <- filter(recruitment_data, bday_18_month < start_date) #use to validate in case on entries before period


#GENERATE DATA AFTER START DATE TO ENSURE YOU ONLY GENERATE NEW ALARMS FOR PARTICIPANTS THAT HAVE NOT HAD ANY VISITS SINCE THE START
data_completed_after_start_date <- filter(field_data_identified_exited_removed, event_date >= start_date)[, c('hf',
                                                                                                              'study_number',
                                                                                                              'event_date',
                                                                                                              'instrument')]


# DATA AFTER PERIOD STARTED -----------------------------------------------

    # BEFORE, ONLY DATA COMPLETED WITHIN PERIOD WERE CONSIDERED BUT THAT WOULD NOT HOLD FOR DATA COMPLETED AFTER THE PERIOD --------
    #ALL DATA WITH THE POTENTIAL TO FULFIL THE REASON FOR WHICH THE ALARM WAS SET OFF WILL BE CONCATENATED WITH ELIGIBLE FRAME CONTAINING THE DATE THE 
    #ALARM WAS SET OFF. THE EARLIEST DATA COMPLETED AFTER THE ALARM DATE WILL BE USED TO DETERMINE WHAT DISABLED THE ALARM.
    # THEREFORE, THE VARIOUS STOP-ABLE DATA WILL BE GENERATED AND MERGED WITH THE ELIGIBLE ONEs:
    # surveillance calls- all data (more or less)
    #surveillance house - all data (more or less)
              #find out whether a call was done as house and vice versa}
    #MRV2 - MRV2, M18, death, withdrawal
                #ALTHOUGH M18 SHOULD NOT BE COMPLETED WHEN IT HAS NOT BEEN MOVED TO MRV2 YET
    #M18 - M18, death, withdrawal
    #THINK ABOUT SURVEILLANCE THAT MAY TURN TO M18 OR MRV AND MRV2 THAT MAY TURN TO M18
    #ANOTHER CONCERN IS THAT YOU MAY SEE DATA COMPLETED DURING A PARTICULAR THAT WAS MEANT FOR  PERIOD PRIOR TO THE ONE UNDER CONSIDERATION
      #FOR THIS SURVEILLANCE WILL NOT POSE MUCH PROBLEMS BUT MRV2 AND M18 MAY BE A LIL CHALLENGING TRYING TO ASCERTAIN WHETHER THEY WERE PEDING OR NOT
      #"eligible_period... have been adjusted to consider all eligible by end of period under consideration

#generate all data after the start of the period and separate into various stopping groups
potential_period_related_data <- field_data_identified[field_data_identified$event_date >= start_date, ]

#MRV2 pending
potential_period_mrv2_stopper <- filter(recruitment_data, bday_15_month >= start_date)  

#M18 pending
potential_period_m18_stopper <- filter(recruitment_data, bday_18_month >= start_date)

#change codes in surv and m18
#
#data frame categories:
#alarms
#eligible
#completed
#google

# ||||||||||||||||||||||||||||||------------------------------------------------------


#field_data$phone_contact <- paste(field_data$phone_1,
 #                                 field_data$phone_2,
  #                                sep = "_")

#participants_contacts <- filter(field_data_identified, !is.na(study_number))[
 # , c('study_number',
  #    'phone_contact')
#]



#___________________________Create Google links whn th month turns___________

#data$event_date <- paste0(data[c("sae_awareness_date",
 #                                                     'unsch_visit_date')], na.rm = T)

fu_google_sheets <- list(Surveillance_Calls = c('study_number',
                                                'Assignee',
                                                'surv_due_date',
                                                'call_successful',
                                                'hand_over',
                                                'call_details_if_not_successful',
                                                'completed_in_redcap',
                                                'type_of_visit',
                                                'ms_status_child',
                                                'ms_interviewer_id',
                                                'presumptive_completion'),
                         
                         Surveillance_House = c('study_number',
                                                'Assignee',
                                                'Assignation_date',
                                                'visit_completed_in_redcap',
                                                'visit_completed_by',
                                                'surv_fu_status',
                                                'supported_by_evidence',
                                                'approved_by_coordinator',
                                                'approved_by_data',
                                                'paid',
                                                'amount',
                                                'presumptive_completion'),
                         
                                        M18 =  c('study_number',
                                                 'Assignee',
                                                 'Assignation_date',
                                                 'visit_completed',
                                                 'visit_completed_by',
                                                 'contact_type',
                                                 'm18_fu_status',
                                                 'supported_by_evidence?',
                                                 'approved_by_coordinator',
                                                 'approved_by_data',
                                                 'paid',
                                                 'amount',
                                                 'presumptive_completion'),
                         
                         
                                        MRV2 =c('study_number',
                                                'Assignee',
                                                'Assignation_date',
                                                'type_of_visit',
                                                'paid',
                                                'amount',
                                                'presumptive_completion')
                         )




surv_contact_type <- c('Phone Call',
                       'Household visit')

child_seen <- c('not_seen',
                'seen')

surv_fu_status <- c('Alive',
                    'Dead',
                    'Migrated',
                    'Unreachable',
                    'Visit completed',
                    'Withdrawal')

m18_contact_type <- c('Phone Call',
                      'Household visit',
                      'Health facility visit')

m18_call_status <- c('Alive',
                     'Dead',
                     'Admitted to hospital',
                     'Migrated')

m18_house_status <- c('Reachable',
                      'Unreachable')

contact_fields <- c('child_name',
                    'caretaker_name',
                    'caretaker_relationship',
                    'phone_numbers',
                    'phone_contacts',
                    'address')


icaria_fu_tracker_function <- function(google_data,
                                       sheet_fields,
                                       wrangle_flag,
                                       completed_data,
                                       completion_date,
                                       redcap_alarms,
                                       #activities,
                                       eligible_list,
                                       period_specific_eligible_list,
                                       eligible_date,
                                       stopping_frame,
                                       relevant_stopper_fields,
                                       contacts = NULL,
                                       data_after_start_date = NULL){
  #wrangle_flag is used to separate the closed cases from the yet to be solved
  #google_dta is the data in the google sheet being called into r
  #activities is the frame of all the latest actions in redcap
  #eligible list is the list of pall participants eligible for a particular set of the alarms
  #The stopping frame is already generated and will be used to determine the the latest activities in REDCap
    #It has:  ("study_number","instrument","event_date","interviewer","contact_type","status")
  #activity_order is the sorter...prioritizing M_surv in case multiple instruments were completed on the same date
  #relevant_stopper_fields... fields that should not be dropped after wangling that data in the googleframe
  
  require(dplyr)
  
  closed_google <- data.frame() #_____(1)
  gs_alarm_yet_pending <- data.frame() #___________4
  disabled_alarm_updated <- data.frame() #(2)  - to update the columns
  disabled_absent_in_eligible_or_completed <- data.frame() #3
  acceptable_presumption <- data.frame() #____5
  #unacceptable_presumption <- data.frame() #____6
  pending_alarms <- data.frame()
  
  #return(google_data)
  if(nrow(google_data) > 0){
    #gs_closed_google <- google_data[!is.na(google_data[[wrangle_flag]]), ]
    #closed_google <- rbind(closed_google,
     #                      gs_closed_google)
    
    #gs_open_google <- google_data[is.na(google_data[[wrangle_flag]]), ]
    
    #if(nrow(google_data) >0){
      gs_open_google <- google_data # The codes in the lines above will be ignores because it is vital to enure that evry data is still valid everyday
      #This will alert the team in case some previously valid data was deleted 
      filter_pending_check <- gs_open_google[['study_number']] %in% redcap_alarms[['study_number']]
      
      gs_alarm_lingers <- gs_open_google[filter_pending_check, ]
      
      #if('estimated_next_visit' %in% colnames(gs_open_google)){
       # gs_alarm_lingers <- select(gs_alarm_lingers, -'estimated_next_visit')
        #gs_alarm_lingers <- left_join(gs_alarm_lingers,
         #                             next_visit_frame)
      #}
      
      gs_alarm_yet_pending <- rbind(gs_alarm_yet_pending,
                                    gs_alarm_lingers)
      
      
      gs_alarm_disabled <- gs_open_google[!filter_pending_check, ]
      
      if(nrow(gs_alarm_disabled) > 0){
        
        #Disabled absent in one of stopper / eligible
        
        #stopping_frame <- stopping_frame[, c('study_number',
        #                                relevant_stopper_fields)]
        
        not_in_eligible_or_stopper_filter <- !(gs_alarm_disabled[['study_number']] %in% eligible_list[['study_number']] |
                                                 gs_alarm_disabled[['study_number']] %in% stopping_frame[['study_number']])
        
        not_in_eligible_or_stopper <- gs_alarm_disabled[not_in_eligible_or_stopper_filter, ]
        
        if(nrow(not_in_eligible_or_stopper) > 0){
          not_in_eligible_or_stopper[['presumptive_completion']] <- "possible_redcap_error"
          
          disabled_absent_in_eligible_or_completed <-rbind(disabled_absent_in_eligible_or_completed,
                                                           not_in_eligible_or_stopper
          )
        }
        
        #Disabled present in both of stopper & eligible
        gs_alarm_disabled <- select(gs_alarm_disabled, -relevant_stopper_fields)
        
        gs_disabled_alarm_in_eligible <- eligible_list[eligible_list[['study_number']] %in%
                                                         gs_alarm_disabled[['study_number']],
                                                       c("study_number",
                                                         eligible_date)
        ]
        
        
        
        gs_disabled_alarm_in_stopping_frame <- stopping_frame[stopping_frame[['study_number']] %in%
                                                                gs_alarm_disabled[['study_number']], ]
        
        specific_stopper <- full_join(gs_disabled_alarm_in_eligible,
                                      gs_disabled_alarm_in_stopping_frame,
                                      by = "study_number")
        
        
        specific_stopper$period_bwn_eligible_and_post_event <- interval(specific_stopper[[eligible_date]], 
                                                                        specific_stopper$valid_latest_activity_date) %/% days(1)
        
        
        
        specific_stopper_negatives_eliminated <- filter(specific_stopper, period_bwn_eligible_and_post_event >= 0)
        
        specific_stopper_negatives_eliminated <- group_by(specific_stopper_negatives_eliminated, study_number)
        
        #considering the earliest disabler of the alarm:
        earliest_stoppers <- filter(specific_stopper_negatives_eliminated, period_bwn_eligible_and_post_event == min(period_bwn_eligible_and_post_event, na.rm = T))
        
        earliest_stoppers <- earliest_stoppers[order(factor(earliest_stoppers$valid_latest_activity, levels = activity_order)),]
        
        the_earliest_stopper <- earliest_stoppers[!duplicated(earliest_stoppers$study_number), ]
        
        the_earliest_stopper <- merge(the_earliest_stopper,
                                      gs_alarm_disabled)
        
        disabled_alarm_updated <- rbind(disabled_alarm_updated,
                                        the_earliest_stopper)
        
        disabled_alarm_updated[['due_date']] <- disabled_alarm_updated[[eligible_date]]
        disabled_alarm_updated <- select(disabled_alarm_updated, -eligible_date)
        disabled_alarm_updated <- disabled_alarm_updated[, colnames(gs_open_google)]
        
        #colnames(disabled_alarm_updated) <- disabled_alarm_updated[order(factor(colnames(disabled_alarm_updated), levels =paste(unlist(colnames(google_data))), ]
        
        #colnames(disabled_alarm_updated) <- stopping_frame[order(factor(colnames(disabled_alarm_updated), levels = paste(unlist(colnames(google_data))))),] #_____________
        
        
        #stopping_frame[order(factor(stopping_frame$instrument, levels = activity_order)),]
        
        ###to work on the data verification
        ###work on getting order of the columns right
      }
      
    }
    #return(disabled_alarm_updated)
    #remember to include next visit date for the surveillance
  #}

   
    #if(nrow(gs_open_google) > 0){

      
  
  #Now checking for data in recap that is not in google & or / eligible list
  
  if(nrow(completed_data) > 0){
    if(start_date == "2024-05-01"){
      presumption <- completed_data[(completed_data[['study_number']] %in% eligible_list[['study_number']]) &
                                      !(completed_data[['study_number']] %in% google_data[['study_number']]), ]
      if(nrow(presumption) > 0){
        presumption <- merge(presumption, eligible_list)
        correct_presumption <- filter(presumption, (completion_date >= eligible_date))# & eligible_date <= end_date))
        if(nrow(correct_presumption) > 0){
          correct_presumption[['presumptive_completion']] <- "presumptive but correct"
          correct_presumption[['due_date']] <- correct_presumption[[eligible_date]]
          correct_presumption <- select(correct_presumption, -c(eligible_date, 'instrument'))
          acceptable_presumption <- rbind(acceptable_presumption,
                                          correct_presumption
          )
        }
      }
    }else{
      presumption <- completed_data[(completed_data[['study_number']] %in% period_specific_eligible_list[['study_number']]) &
                                      !(completed_data[['study_number']] %in% google_data[['study_number']]), ]
      if(nrow(presumption) > 0){
        presumption <- merge(presumption, period_specific_eligible_list)
        correct_presumption <- filter(presumption, (completion_date >= eligible_date ))
        if(nrow(correct_presumption) > 0){
          correct_presumption[['presumptive_completion']] <- "presumptive but correct"
          correct_presumption[['due_date']] <- correct_presumption[[eligible_date]]
          correct_presumption <- select(correct_presumption, -c(eligible_date, 'instrument'))
          acceptable_presumption <- rbind(acceptable_presumption,
                                          correct_presumption
          )
        }
        
      }
    }
   
    
    #wrong_presumption <- completed_data[!(completed_data[['study_number']] %in% eligible_list[['study_number']]) & 
     #                                     !(completed_data[['study_number']] %in% google_data[['study_number']]), ]
    #if(nrow(wrong_presumption) > 0){
     # wrong_presumption[['presumptive_completion']] <- "presumptive and possible_staff_error"
      #unacceptable_presumption <- rbind(unacceptable_presumption,
      #                                  wrong_presumption)
    #}
    
  }
  
  verifiables <- bind_rows(disabled_alarm_updated,
                          acceptable_presumption)
  
  if(nrow(redcap_alarms) > 0){
    new_alarms <- redcap_alarms[!(redcap_alarms[['study_number']] %in% google_data[['study_number']]) &
                                  !(redcap_alarms[['study_number']] %in% data_after_start_date[['study_number']]), ]
    if(nrow(new_alarms) > 0){
      if(start_date == "2024-05-01"){
        eligible_list <- eligible_list[, c('study_number',
                                            eligible_date)]
        
                colnames(eligible_list) <- c('study_number',
                                     'due_date')
        
        eligible_list <- filter(eligible_list, due_date <= "2024-05-31")
        
        new_alarms <- merge(new_alarms, eligible_list)
      }else{
        period_specific_eligible_list <- period_specific_eligible_list[, c('study_number',
                                                                           eligible_date)]
        colnames(period_specific_eligible_list) <- c('study_number',
                                                     'due_date')
        
        new_alarms <- merge(new_alarms, period_specific_eligible_list) #[, c('study_number',
        #eligible_date)]
        #               )
      }
      
      #remove unwanted columns
      unwanteds <- c('hf', 'phone_contact')
      for(unwanted in unwanteds){
        if(unwanted %in% colnames(new_alarms)){
          new_alarms <- select(new_alarms, -unwanted)
        }
      }
      
      #new_alarms <- new_alarms[, c('study_number',
       #                            eligible_date,
        #                           )]
      
      #colnames(new_alarms) <- c('study_number',
       #                            'due_date')

      #if('estimated_next_visit' %in% colnames(gs_google)){#gs_google replaced gs_open_google
      #  new_alarms <- left_join(new_alarms,
      #                          next_visit_frame)
      #}
      
      #if(stopping_frame != "surveillance_stopping_frame"){
       # new_alarms <- left_join(new_alarms,
        #                        next_visit_frame)
      #}
      
  pending_alarms <- rbind(pending_alarms,
                            new_alarms)
    }
  }
  
  #return(disabled_absent_in_eligible_or_completed)
  
  #return(gs_disabled_alarm_in_stopping_frame)
  
  #consider making the presump...field as part of the exclusion criteria to not wrangle
  
  to_google_frames <- list(#sheet_fields,
                         disabled_alarm_updated,
                         acceptable_presumption,
                         #disabled_absent_in_eligible_or_completed,
                         gs_alarm_yet_pending,
                         pending_alarms)
  
  to_google <- data.frame()
  to_google <- bind_rows(to_google,
                         sheet_fields)
  
  for(frame in to_google_frames){
    if(nrow(frame) > 0){
      to_google <- bind_rows(to_google,
                             frame)
    }
  }
  
  to_google <- to_google[order(factor(to_google$valid_latest_activity, levels = activity_order)),]
  to_google <- to_google[!(duplicated(to_google[['study_number']])), ]
  
  return(to_google)
}


#NEXT VISIT CALCULATION

   #Get all EPI scheduled visits
epi_visits <- filter(field_data_identified_exited_removed, redcap_event_name %in%names(epi_event_identifiers) & !is.na(int_date))

   #Find the latest epi event
epi_visits_grouped <- group_by(epi_visits, study_number)
latest_epi_visits <- filter(epi_visits_grouped, int_date == max(int_date, na.rm =T))

latest_epi_visits <- latest_epi_visits[, c('hf',
                                           'study_number',
                                           'instrument',
                                           'event_date',
                                           #'int_vacc_rtss1',
                                           #'int_vacc_rtss1_date',
                                           #'int_vacc_rtss2',
                                           #'int_vacc_rtss3',
                                           'event_age',
                                           'child_dob')]
colnames(latest_epi_visits) <- c('hf',
                           'study_number',
                           'epi_instrument',
                           'epi_event_date',
                           'epi_event_age',
                           'child_dob')


#Get all rtss visits
rtss_visits <- filter(field_data_identified_exited_removed, instrument == 'rtss' & !is.na(rtss_date))
rtss_visits <- rtss_visits[, c('study_number',
                               rtss_fields)] #Do cleaning on this

write.csv(rtss_visits, "rtss_visits.csv", row.names = F)
#Focus on the actual administrations 



rtss_regimens <-c('rtss_vacc_rtss1_date',
                  'rtss_vacc_rtss2_date',
                  'rtss_vacc_rtss3_date',
                  'rtss_vacc_rtss4_date') 

#Isolate the first two administrations from the rest

first_two_rtss <- filter(rtss_visits, rtss_vacc_rtss1 ==TRUE | rtss_vacc_rtss2 == TRUE)

first_two_rtss <- first_two_rtss[, c("study_number",
                                     "rtss_date",
                                     "rtss_vacc_rtss1",
                                     "rtss_vacc_rtss2")]

third_rtss <- filter(rtss_visits, rtss_vacc_rtss3 == TRUE)
third_rtss <- third_rtss[, c("study_number",
                             "rtss_date",
                             "rtss_vacc_rtss3")]

fourth_rtss <- filter(rtss_visits, rtss_vacc_rtss4 == TRUE)
fourth_rtss <- fourth_rtss[, c("study_number",
                               "rtss_date",
                               "rtss_vacc_rtss3")]

#narrow down to thse with only two doses
first_two_rtss_only <- first_two_rtss[!(first_two_rtss$study_number %in% third_rtss$study_number |
                                          first_two_rtss$study_number %in% fourth_rtss$study_number), ]

first_two_rtss_only_grouped <- group_by(first_two_rtss_only, study_number)
latest_first_two_rtss_only <- filter(first_two_rtss_only_grouped, rtss_date == max(rtss_date, na.rm =T))

#RTSS has to be treated differently
# The first two doses require 1 month next isit
#3rd dose reception means that the next dose would have to be administered at 18 month old at least
#Therefore isolate the first two doses

#19-08-2024 The 3rd and 4th doses of rtss are not really consequential to the calculation of next visit.
# Because after 3rd dose is the 4th dose which is received at 18months old - ICARIA endline 

#Find the latest rtss event
#rtss_visits_grouped <- group_by(rtss_visits, study_number)
#latest_rtss_visits <- filter(rtss_visits_grouped, rtss_date == max(rtss_date, na.rm =T))




#Join the visits
latest_visits <- left_join(latest_epi_visits,
                           latest_first_two_rtss_only)

latest_visits <- latest_visits[order(factor(latest_visits$epi_instrument, levels = activity_order)), ]


latest_visits <- latest_visits[!duplicated(latest_visits$study_number), ]

epi_visit_age_dependants <-   c(6,9,12,15,18)

vaccine_event_relationships <- list(
  VitA1 = 'int_vacc_rtss1',
  MalVacc2 = 'int_vacc_rtss2',
  MalVacc3 = 'int_vacc_rtss3'
)



#     
# next_visit_calc <- function(latest_scheduled_visits, age_thresholds){
#   
#   require(dplyr)
#   require(tidyr) 
#   require(lubridate)
#   
#   single_month_next_visits_event <- c('Penta1',
#                                       'Penta2',
#                                       'MalVacc2',
#                                       'MalVacc3')
#   
#   simple_3_months_events <- c('Penta3',
#                               'MRV1',
#                               'VitA2')
#   
#   multi_possibilities <- c('VitA1')
#   
#   next_visit_pointer <- list(
#     Penta3 = 'month_6_bday',
#     VitA1 = 'month_9_bday',
#     MRV1 = 'month_12_bday',
#     VitA2 = 'month_15_bday'
#   )
#   
#   for(epi_month in age_thresholds){
#     latest_scheduled_visits[[paste0('month_',epi_month,'_bday')]] <- ymd(latest_scheduled_visits[['child_dob']]) %m+% months(epi_month)
#   }
#   
#   next_visit_all <- data.frame()
#   
#   #One month difference
#   
#   one_month_filter <- (latest_scheduled_visits[['instrument']] %in% single_month_next_visits_event) |
#                          (latest_scheduled_visits[['instrument']] == 'VitA1'  & !is.na(latest_scheduled_visits[['int_vacc_rtss1_date']])) &
#                          !is.na(latest_scheduled_visits[['instrument']])
#     
#   one_month_set <- latest_scheduled_visits[one_month_filter, ]
#   
#   one_month_set_vitA <- one_month_set[one_month_set[['instrument']] == 'VitA1' & !is.na(one_month_set[['instrument']]), ]
#   one_month_set_vitA[['estimated_next']] <- ymd(one_month_set_vitA[['int_vacc_rtss1_date']]) %m+% months(1) #____________1
#   
#   one_month_set_not_vitA <- one_month_set[one_month_set[['instrument']] != 'VitA1' & !is.na(one_month_set[['instrument']]), ]
#   one_month_set_not_vitA[['estimated_next']] <- ymd(one_month_set_not_vitA[['event_date']]) %m+% months(1) #____________2
#   
#   three_month_filter <- (latest_scheduled_visits[['instrument']] %in% names(next_visit_pointer) & !is.na(latest_scheduled_visits[['instrument']]))
#   three_month_set <- latest_scheduled_visits[three_month_filter, ]
#   
#   #Three month difference
#   
#   three_months_category <- data.frame()
#   for(three_month_related in names(next_visit_pointer)){
#     if(three_month_related == 'VitA1'){
#       three_month_set_type <- three_month_set[three_month_set[['instrument']] == three_month_related & !is.na(three_month_set[['instrument']]) & (is.na(three_month_set[['int_vacc_rtss1_date']])), ]
#     }else{
#       three_month_set_type <- three_month_set[three_month_set[['instrument']] == three_month_related & !is.na(three_month_set[['instrument']]), ]
#     }
#     #three_month_set_type <- three_month_set[three_month_set[['instrument']] == three_month_related & !is.na(three_month_set[['instrument']]), ]
#     
#     bday <- next_visit_pointer[[three_month_related]]
#     
#     #return(class(three_month_set_type[['event_date']]))
#     three_month_set_type[['date_difference']] <- interval(three_month_set_type[['event_date']], as.Date(three_month_set_type[[bday]]))  %/% months(1)
    
    # #three_month_set_type[['estimated_next']]
    # three_month_normal_filter <-  three_month_set_type['date_difference'] > 1
    # three_month_late_filter <-  three_month_set_type['date_difference'] <= 1
    # 
    #three_month_set_type[three_month_normal_filter, 'estimated_next'] <- three_month_set_type[three_month_normal_filter, bday]
    #three_month_set_type[three_month_late_filter, 'estimated_next'] <- as.data.frame(three_month_set_type[three_month_late_filter, 'event_date']) + 30 #%m+% months(1)
    
    #three_month_set_type <- select(three_month_set_type, -'date_difference')
    #three_months_category <- rbind(three_months_category,
   #                                three_month_set_type) #_______________3
    
  #}
  
  #Vit A situation that can be either 3 moth or one month difference determined by RTSS
  #vitA_filter <- (latest_scheduled_visits[['instrument']] %in% multi_possibilities & !is.na(latest_scheduled_visits[['instrument']]))
  #vitA_set <- latest_scheduled_visits[vitA_filter, ]

  #MRV2
  #mrv2_filter <- latest_scheduled_visits[['instrument']] == "MRV2"
  #mrv_2_set <- latest_scheduled_visits[mrv2_filter, ]
  #mrv_2_set[['estimated_next']] <- as.Date(NA)
  
  
  
  #next_visit_all <- rbind(one_month_set_vitA,
   #                       one_month_set_not_vitA,
    #                      three_months_category,
     #                     mrv_2_set)
  
  #Adjust for weekends in other HFs other than HF08, HF16, HF17  
  
  #next_visit_all$weekday <- lubridate::wday(next_visit_all$estimated_next)
  
  #saturday_hfs <- c('HF08',
    #                'HF16',
   #                 'HF17')
  
  
  #saturday_working_hfs_filter <- (next_visit_all[['hf']] %in% saturday_hfs) & (next_visit_all[['weekday']] != 1) &
 #   (!is.na(next_visit_all[['weekday']])) 
  
  #sunday_filter <- (next_visit_all[['weekday']] == 1) & (!is.na(next_visit_all[['weekday']]))
  
 # non_saturday_working_hfs_filter <- !(next_visit_all[['hf']] %in% saturday_hfs) & (next_visit_all[['weekday']] == 7) &
#    (!is.na(next_visit_all[['weekday']]) )
  
  #non_saturday_hfs_weekday <- !(next_visit_all[['hf']] %in% saturday_hfs) & (next_visit_all[['weekday']] != 1| next_visit_all[['weekday']] != 7)&
   # (!is.na(next_visit_all[['weekday']]))
  #
  #
  #next_visit_all[(saturday_working_hfs_filter | non_saturday_hfs_weekday), "estimated_next_visit"] <- (next_visit_all[(saturday_working_hfs_filter | non_saturday_hfs_weekday), 'estimated_next'])
 # 
#  next_visit_all[sunday_filter, "estimated_next_visit"] <- as.data.frame(next_visit_all[sunday_filter, 'estimated_next']) + 1
#  
#  next_visit_all[non_saturday_working_hfs_filter, "estimated_next_visit"] <- as.data.frame(next_visit_all[non_saturday_working_hfs_filter, 'estimated_next']) + 2
#  
#  
#  next_visit_all <- next_visit_all[, c('study_number',
#                                       'estimated_next_visit')]
#
#  
 # return(next_visit_all)
#}



next_visit_calc <- function(latest_scheduled_visits, age_thresholds){
 
   #For this new function, the last epi will be joined with the last rtss
  
  
  
  require(dplyr)
  require(tidyr) 
  require(lubridate)
  
  single_month_next_visits_event <- c('Penta1',
                                      'Penta2')

  
  simple_3_months_events <- c('Penta3')
  
  multi_possibilities <- c('VitA1',
                           'MRV1',
                           'VitA2',
                           'MRV2'
  )
  
  next_visit_pointer <- list(
    Penta3 = 'month_6_bday',
    VitA1 = 'month_9_bday',
    MRV1 = 'month_12_bday',
    VitA2 = 'month_15_bday',
    MRV2 = 'month_18_bday'
  )
  
  for(epi_month in age_thresholds){
    latest_scheduled_visits[[paste0('month_',epi_month,'_bday')]] <- ymd(latest_scheduled_visits[['child_dob']]) %m+% months(epi_month)
  }
  
  next_visit_all <- data.frame()
  
  #One month difference
  
  one_month_filter <- (latest_scheduled_visits[['epi_instrument']] %in% single_month_next_visits_event) &  #This was simple_3_months_events instead of single_month_next_visits_event
    !is.na(latest_scheduled_visits[['epi_instrument']])
  
  one_month_set <- latest_scheduled_visits[one_month_filter, ]
  
  if(nrow(one_month_set) > 0){
    one_month_set[['estimated_next']] <- ymd(one_month_set[['epi_event_date']]) %m+% months(1) #____________(1)
    
  }
  

  #Penta3 case  
  
  
  # penta3_filter <- (latest_scheduled_visits[['epi_instrument']] %in% single_month_next_visits_event) &
  #   !is.na(latest_scheduled_visits[['epi_instrument']])
  # 
  # penta3_set <- latest_scheduled_visits[penta3_filter, ]
  # 
  # penta3_set[['estimated_next']] <- penta3_set[['month_6_bday']] #____________2 
  
  
  
  #Complex rtss possible cases
  
  three_month_filter <- (latest_scheduled_visits[['epi_instrument']] %in% names(next_visit_pointer) & !is.na(latest_scheduled_visits[['epi_instrument']]))
  three_month_set <- latest_scheduled_visits[three_month_filter, ]
   
      #NO RTSS
  
  no_rtss_3_months_filter <- !is.na(three_month_set[['epi_instrument']]) & is.na(three_month_set[['rtss_date']])
  no_rtss_3_month_set <- three_month_set[no_rtss_3_months_filter, ]
  
  no_rtss_three_months_category <- data.frame()
  for(three_month_related in names(next_visit_pointer)){
      no_rtss_three_month_set_type <- no_rtss_3_month_set[no_rtss_3_month_set[['epi_instrument']] == three_month_related & !is.na(no_rtss_3_month_set[['epi_instrument']]), ]
      
    bday <- next_visit_pointer[[three_month_related]]
    
    #return(class(three_month_set_type[['epi_event_date']]))
    no_rtss_three_month_set_type[['date_difference']] <- interval(no_rtss_three_month_set_type[['epi_event_date']], as.Date(no_rtss_three_month_set_type[[bday]]))  %/% months(1)
    
    #three_month_set_type[['estimated_next']]
    no_rtss_three_month_normal_filter <-  no_rtss_three_month_set_type['date_difference'] >= 1
    no_rtss_three_month_late_filter <-  no_rtss_three_month_set_type['date_difference'] < 1
    
    if(nrow(no_rtss_three_month_set_type[no_rtss_three_month_normal_filter, ]) >0 ){
      
      no_rtss_three_month_set_type[no_rtss_three_month_normal_filter, 'estimated_next'] <- no_rtss_three_month_set_type[no_rtss_three_month_normal_filter, bday]
      
    }
    
    if(nrow(no_rtss_three_month_set_type[no_rtss_three_month_late_filter, ]) >0 ){
      no_rtss_three_month_set_type[no_rtss_three_month_late_filter, 'estimated_next'] <- as.data.frame(no_rtss_three_month_set_type[no_rtss_three_month_late_filter, 'epi_event_date']) + 30 #%m+% months(1)
      
    }
    
    no_rtss_three_month_set_type <- select(no_rtss_three_month_set_type, -'date_difference')
    no_rtss_three_months_category <- rbind(no_rtss_three_months_category,
                                           no_rtss_three_month_set_type) #_______________(2)
    
  }
  
  
  
  # RTSS RECEIVED
  
  with_rtss_3_months_filter <- !is.na(three_month_set[['epi_instrument']]) & !is.na(three_month_set[['rtss_date']])
  with_rtss_3_month_set <- three_month_set[with_rtss_3_months_filter, ]
  
  with_rtss_three_months_category <- data.frame()
  
  
  for(three_month_related in names(next_visit_pointer)){
    #if(three_month_related == 'Penta3'){
     # next
    #}
    with_rtss_three_month_set_type <- with_rtss_3_month_set[with_rtss_3_month_set[['epi_instrument']] == three_month_related & !is.na(with_rtss_3_month_set[['epi_instrument']]), ]
    
    bday <- next_visit_pointer[[three_month_related]]
    
    #return(class(three_month_set_type[['epi_event_date']]))
    with_rtss_three_month_set_type[['date_difference']] <- interval(with_rtss_three_month_set_type[['epi_event_date']], as.Date(with_rtss_three_month_set_type[[bday]]))  %/% months(1)
    
    rtss_three_month_normal_filter <-  with_rtss_three_month_set_type['date_difference'] > 1
    rtss_three_month_late_filter <-  with_rtss_three_month_set_type['date_difference'] <= 1
    
    if(nrow(with_rtss_three_month_set_type[rtss_three_month_normal_filter, ]) > 0){
      
      with_rtss_three_month_set_type[rtss_three_month_normal_filter, 'epi_estimated_next'] <- with_rtss_three_month_set_type[rtss_three_month_normal_filter, bday]
      
    }
    
    if(nrow(with_rtss_three_month_set_type[rtss_three_month_late_filter, ]) > 0){
      
      with_rtss_three_month_set_type[rtss_three_month_late_filter, 'epi_estimated_next'] <- as.data.frame(with_rtss_three_month_set_type[rtss_three_month_late_filter, 'epi_event_date']) + 30 #%m+% months(1)
      
    }
    
    
    with_rtss_three_month_set_type[['rtss_estimated_next']] <- ymd(as.Date(with_rtss_three_month_set_type[['rtss_date']])) %m+% months(1)
    
    
    rtss_next_after_or_exact_epi_next_filter <- with_rtss_three_month_set_type[['epi_estimated_next']] <= with_rtss_three_month_set_type[['rtss_estimated_next']]
    rtss_next_before_epi_next_filter  <- with_rtss_three_month_set_type[['epi_estimated_next']] > with_rtss_three_month_set_type[['rtss_estimated_next']]
#  }
 # return(with_rtss_three_month_set_type)
#}   

    if(nrow(with_rtss_three_month_set_type[rtss_next_after_or_exact_epi_next_filter, ]) > 0){
      
      with_rtss_three_month_set_type[rtss_next_after_or_exact_epi_next_filter, 'estimated_next'] <- with_rtss_three_month_set_type[rtss_next_after_or_exact_epi_next_filter, 'epi_estimated_next']
      
    }
    
    with_rtss_three_month_set_type[rtss_next_before_epi_next_filter, 'estimated_next'] <- with_rtss_three_month_set_type[rtss_next_before_epi_next_filter, 'rtss_estimated_next']
    
    with_rtss_three_month_set_type <- select(with_rtss_three_month_set_type, -c('date_difference', 'epi_estimated_next', 'rtss_estimated_next'))
    
    with_rtss_three_months_category <- rbind(with_rtss_three_months_category,
                                             with_rtss_three_month_set_type) #_______________(3)
    
  }
  
  
  
  
  
 #do the if and month 18 
  # 
  # three_months_category <- data.frame()
  # for(three_month_related in names(next_visit_pointer)){
  #   if(three_month_related == 'VitA1'){
  #     three_month_set_type <- three_month_set[three_month_set[['epi_instrument']] == three_month_related & !is.na(three_month_set[['epi_instrument']]) & (is.na(three_month_set[['int_vacc_rtss1_date']])), ]
  #   }else{
  #     three_month_set_type <- three_month_set[three_month_set[['epi_instrument']] == three_month_related & !is.na(three_month_set[['epi_instrument']]), ]
  #   }
  #   #three_month_set_type <- three_month_set[three_month_set[['epi_instrument']] == three_month_related & !is.na(three_month_set[['epi_instrument']]), ]
  #   
  #   bday <- next_visit_pointer[[three_month_related]]
  #   
  #   #return(class(three_month_set_type[['epi_event_date']]))
  #   three_month_set_type[['date_difference']] <- interval(three_month_set_type[['epi_event_date']], as.Date(three_month_set_type[[bday]]))  %/% months(1)
  #   
  #   #three_month_set_type[['estimated_next']]
  #   three_month_normal_filter <-  three_month_set_type['date_difference'] > 1
  #   three_month_late_filter <-  three_month_set_type['date_difference'] <= 1
  #   
  #   three_month_set_type[three_month_normal_filter, 'estimated_next'] <- three_month_set_type[three_month_normal_filter, bday]
  #   three_month_set_type[three_month_late_filter, 'estimated_next'] <- as.data.frame(three_month_set_type[three_month_late_filter, 'epi_event_date']) + 30 #%m+% months(1)
  #   
  #   three_month_set_type <- select(three_month_set_type, -'date_difference')
  #   three_months_category <- rbind(three_months_category,
  #                                  three_month_set_type) #_______________3
  #   
  # }
  # 
  #Vit A situation that can be either 3 moth or one month difference determined by RTSS
  #vitA_filter <- (latest_scheduled_visits[['epi_instrument']] %in% multi_possibilities & !is.na(latest_scheduled_visits[['epi_instrument']]))
  #vitA_set <- latest_scheduled_visits[vitA_filter, ]
  
  # #MRV2
  # mrv2_filter <- latest_scheduled_visits[['epi_instrument']] == "MRV2"
  # mrv_2_set <- latest_scheduled_visits[mrv2_filter, ]
  # mrv_2_set[['estimated_next']] <- as.Date(NA)
  
  
  
  next_visit_all <- rbind(one_month_set,
                          no_rtss_three_months_category,
                          with_rtss_three_months_category
                          )
                          #mrv_2_set)
  
  #Checking wheter any next visit is after month 18
  
  next_visit_after_m18_filter <- next_visit_all[['month_18_bday']] <= next_visit_all[['estimated_next']]

  next_visit_all[next_visit_after_m18_filter, 'estimated_next'] <- next_visit_all[next_visit_after_m18_filter, 'month_18_bday']

  
  #Adjust for weekends in other HFs other than HF08, HF16, HF17  
  
  next_visit_all$weekday <- lubridate::wday(next_visit_all$estimated_next)
  
  saturday_hfs <- c('HF08',
                    'HF16',
                    'HF17')
  
  
  saturday_working_hfs_filter <- (next_visit_all[['hf']] %in% saturday_hfs) & (next_visit_all[['weekday']] != 1) &
    (!is.na(next_visit_all[['weekday']])) 
  
  sunday_filter <- (next_visit_all[['weekday']] == 1) & (!is.na(next_visit_all[['weekday']]))
  
  non_saturday_working_hfs_filter <- !(next_visit_all[['hf']] %in% saturday_hfs) & (next_visit_all[['weekday']] == 7) &
    (!is.na(next_visit_all[['weekday']]) )
  
  non_saturday_hfs_weekday <- !(next_visit_all[['hf']] %in% saturday_hfs) & (next_visit_all[['weekday']] != 1| next_visit_all[['weekday']] != 7)&
    (!is.na(next_visit_all[['weekday']]))
  
  
  next_visit_all[(saturday_working_hfs_filter | non_saturday_hfs_weekday), "estimated_next_visit"] <- (next_visit_all[(saturday_working_hfs_filter | non_saturday_hfs_weekday), 'estimated_next'])
  
  next_visit_all[sunday_filter, "estimated_next_visit"] <- as.data.frame(next_visit_all[sunday_filter, 'estimated_next']) + 1
  
  next_visit_all[non_saturday_working_hfs_filter, "estimated_next_visit"] <- as.data.frame(next_visit_all[non_saturday_working_hfs_filter, 'estimated_next']) + 2
  
  
  next_visit_all <- next_visit_all[, c('study_number',
                                       'estimated_next_visit')]
  
  
  return(next_visit_all)
}

next_visit_frame <- next_visit_calc(latest_visits, epi_visit_age_dependants)




gs_call_surv_fields <- list(redcap_pre_wrangler = c('due_date',
                                                   'next_visit'),
                            
                            supervisors_field = c('Assignee',
                                                  'call_successful',
                                                  'hand_over',
                                                  'call_details_if_not_successful'),
                            
                            redcap_post_wrangler =c('valid_latest_activity',
                                                     'valid_latest_activity_date',
                                                     'status',
                                                     'interviewer_id',
                                                     'presumptive_completion'),
                            verification_fields = c()
                            )
                         

                         
gs_house_surv_fields <- list(redcap_pre_wrangler = c('due_date',
                                                    'next_visit'),
                            
                            supervisors_field = c('Assignee',
                                                  'Assignation_date',
                                                  'call_details_if_not_successful'),
                            
                            redcap_post_wrangler =c('valid_latest_activity',
                                                     'valid_latest_activity_date',
                                                     'interviewer_id',
                                                     'status',
                                                     'presumptive_completion'),
                            
                            verification_fields = c('supported_by_evidence?supported_by_evidence?',
                                                    'approved_by_coordinator',
                                                    'approved_by_data',
                                                    'paid',
                                                    'amount')
                            )



gs_m18_fields <- list(redcap_pre_wrangler = c('due_date'),
                             
                             supervisors_field = c('Assignee',
                                                   'Assignation_date'),
                             
                             redcap_post_wrangler =c('valid_latest_activity',
                                                      'valid_latest_activity_date',
                                                      'interviewer_id',
                                                      'contact_type',
                                                      'status',
                                                      'presumptive_completion'),
                             
                             verification_fields = c('supported_by_evidence?supported_by_evidence?',
                                                     'approved_by_coordinator',
                                                     'approved_by_data',
                                                     'paid',
                                                     'amount')
                             )

gs_mrv2_fields <- list(redcap_pre_wrangler = c('due_date'),
                             
                             supervisors_field = c('Assignee',
                                                   'Assignation_date'),
                             
                             redcap_post_wrangler =c('valid_latest_activity',
                                                      'valid_latest_activity_date',
                                                      'presumptive_completion'),
                             
                             verification_fields = c('paid',
                                                     'amount')
)


surv_relevant_stopper_fields <- c('valid_latest_activity',
                                  'valid_latest_activity_date',
                                  'interviewer_id',
                                  'contact_type',
                                  'status')

m18_relevant_stopper_fields <- c('valid_latest_activity',
                                  'valid_latest_activity_date',
                                  'interviewer_id',
                                  'contact_type',
                                  'status')

mrv2_relevant_stopper_fields <- c('valid_latest_activity',
                                  'valid_latest_activity_date')

#hfs <- c("HF01", "HF04")#, "HF15", "HF16", "HF17")

m_18_cumulative <- data.frame()

for(facility in hfs){
  
  #______________________________________________________________________
  #Generate the google data
  
  hf_link <- link_frame[(link_frame$HF == facility), period_link_column][[1]]
  
  for(fu_google_sheet in names(fu_google_sheets)){
    hf_google_entry <-  range_read(hf_link,
                                   sheet = fu_google_sheet,
                                   range = NULL,
                                   col_names = TRUE,
                                   col_types = NULL,
                                   na = "",
                                   trim_ws = TRUE,
                                   skip = 0,
                                   #n_max = Inf,
                                   #guess_max = min(1000, n_max),
                                   .name_repair = "unique"
    )
    if(fu_google_sheet == "Surveillance_Calls"){
      gs_surveillance_calls <- hf_google_entry
      gs_surveillance_calls <- select(gs_surveillance_calls, -contact_fields)
      
      test_calls_dups <- gs_surveillance_calls[duplicated(gs_surveillance_calls$study_number), ]
      if(nrow(test_calls_dups) > 0){
        print(paste("DUPLICATES IN CALLS OF GOOGLESHEET__________________", facility))
      }
      

     # long_pending <- filter(gs_surveillance_calls, ((tolower(call_successful) != "yes"| is.na(call_successful))  & (tolower(hand_over) != "yes" | is.na(hand_over)) & is.na(valid_latest_activity)))[, c("study_number", "due_date")]
     # 
     # if(nrow(long_pending) > 0){
     #   write.csv(long_pending, paste(facility, "_please focus on.csv"), row.names = F)
     # }
     # 
     # should_be_entered <- filter(gs_surveillance_calls, (tolower(call_successful) == "yes" &is.na(valid_latest_activity)))[, c("study_number", "due_date", 'Assignee', "call_successful",'hand_over', "valid_latest_activity")]
     # if(nrow(should_be_entered) >  0){
     #   write.csv(should_be_entered, paste(facility, "_why_not_entered.csv"), row.names = F)
     # }
#       
#    }
#  }
#}
    
  
        
      #surveillance_calls_sheet
      surv_calls_sheet <- data.frame(matrix(nrow = 0, ncol = length(colnames(gs_surveillance_calls))))
      colnames(surv_calls_sheet) <- colnames(gs_surveillance_calls)
      
      surv_calls_sheet$due_date <- as.Date(surv_calls_sheet$due_date)
      #surv_calls_sheet$Assignation_date <- as.Date(surv_calls_sheet$Assignation_date)
      surv_calls_sheet$valid_latest_activity_date <- as.Date(surv_calls_sheet$valid_latest_activity_date)
      
      surv_calls_sheet$study_number <- as.character(surv_calls_sheet$study_number)
      surv_calls_sheet$estimated_next_visit <- as.Date(surv_calls_sheet$estimated_next_visit)
      surv_calls_sheet$Assignee <- as.character(surv_calls_sheet$Assignee)
      surv_calls_sheet$call_successful <- as.character(surv_calls_sheet$call_successful)
      surv_calls_sheet$hand_over <- as.character(surv_calls_sheet$hand_over)
      surv_calls_sheet$call_details_if_not_successful <- as.character(surv_calls_sheet$call_details_if_not_successful)
      surv_calls_sheet$valid_latest_activity <- as.character(surv_calls_sheet$valid_latest_activity)
      surv_calls_sheet$interviewer_id <- as.character(surv_calls_sheet$interviewer_id)
      surv_calls_sheet$contact_type <- as.character(surv_calls_sheet$contact_type)
      surv_calls_sheet$status <- as.character(surv_calls_sheet$status)
      surv_calls_sheet$presumptive_completion <- as.character(surv_calls_sheet$presumptive_completion)
      
    }else if(fu_google_sheet == "Surveillance_House"){
      gs_surveillance_house <- hf_google_entry
      
      test_house_dups <- gs_surveillance_house[duplicated(gs_surveillance_house$study_number), ]
      if(nrow(test_house_dups) > 0){
        print(paste("DUPLICATES IN HOUSE OF GOOGLESHEET OF__________________", facility))
      }
#    }
#  }
#}      
      #surveillance_house_sheet
      surv_house_sheet <- data.frame(matrix(nrow = 0, ncol = length(colnames(gs_surveillance_house))))
      colnames(surv_house_sheet) <- colnames(gs_surveillance_house)
      
      surv_house_sheet$due_date <- as.Date(surv_house_sheet$due_date)
      surv_house_sheet$Assignation_date <- as.Date(surv_house_sheet$Assignation_date)
      surv_house_sheet$valid_latest_activity_date <- as.Date(surv_house_sheet$valid_latest_activity_date)
      
      surv_house_sheet$study_number <- as.character(surv_house_sheet$study_number)
      surv_house_sheet$estimated_next_visit <- as.Date(surv_house_sheet$estimated_next_visit)
      surv_house_sheet$Assignee <- as.character(surv_house_sheet$Assignee)
      surv_house_sheet$call_details_if_not_successful <- as.character(surv_house_sheet$call_details_if_not_successful)
      surv_house_sheet$valid_latest_activity <- as.character(surv_house_sheet$valid_latest_activity)
      surv_house_sheet$interviewer_id <- as.character(surv_house_sheet$interviewer_id)
      surv_house_sheet$contact_type <- as.character(surv_house_sheet$contact_type)
      surv_house_sheet$status <- as.character(surv_house_sheet$status)
      surv_house_sheet$presumptive_completion <- as.character(surv_house_sheet$presumptive_completion)
      surv_house_sheet$`supported_by_evidence?` <- as.character(surv_house_sheet$`supported_by_evidence?`)
      surv_house_sheet$approved_by_coordinator <- as.character(surv_house_sheet$approved_by_coordinator)
      surv_house_sheet$approved_by_data <- as.character(surv_house_sheet$approved_by_data)
      surv_house_sheet$paid <- as.character(surv_house_sheet$paid)
      surv_house_sheet$amount <- as.character(surv_house_sheet$amount)
      
      
    }else if(fu_google_sheet == "M18"){
      gs_M18 <- hf_google_entry
      
      #M18 sheet
      #surveillance_house_sheet
      M18_sheet <- data.frame(matrix(nrow = 0, ncol = length(colnames(gs_M18))))
      colnames(M18_sheet) <- colnames(gs_M18)
      
      M18_sheet$due_date <- as.Date(M18_sheet$due_date)
      M18_sheet$Assignation_date <- as.Date(M18_sheet$Assignation_date)
      M18_sheet$valid_latest_activity_date <- as.Date(M18_sheet$valid_latest_activity_date)
      
      M18_sheet$study_number <- as.character(M18_sheet$study_number)
      M18_sheet$Assignee <- as.character(M18_sheet$Assignee)
      #M18_sheet$call_details_if_not_successful <- as.character(M18_sheet$call_details_if_not_successful)
      M18_sheet$valid_latest_activity <- as.character(M18_sheet$valid_latest_activity)
      M18_sheet$interviewer_id <- as.character(M18_sheet$interviewer_id)
      M18_sheet$contact_type <- as.character(M18_sheet$contact_type)
      M18_sheet$status <- as.character(M18_sheet$status)
      M18_sheet$presumptive_completion <- as.character(M18_sheet$presumptive_completion)
      M18_sheet$`supported_by_evidence?` <- as.character(M18_sheet$`supported_by_evidence?`)
      M18_sheet$approved_by_coordinator <- as.character(M18_sheet$approved_by_coordinator)
      M18_sheet$approved_by_data <- as.character(M18_sheet$approved_by_data)
      M18_sheet$paid <- as.character(M18_sheet$paid)
      M18_sheet$amount <- as.character(M18_sheet$amount)
      
      
    }else if(fu_google_sheet == "MRV2"){
      gs_MRV2 <- hf_google_entry
      
      #mrv2 sheet
      
      mrv2_sheet <- data.frame(matrix(nrow = 0, ncol = length(colnames(gs_MRV2))))
      colnames(mrv2_sheet) <- colnames(gs_MRV2)
      
      mrv2_sheet$due_date <- as.Date(mrv2_sheet$due_date)
      mrv2_sheet$Assignation_date <- as.Date(mrv2_sheet$Assignation_date)
      mrv2_sheet$valid_latest_activity_date <- as.Date(mrv2_sheet$valid_latest_activity_date)
      
      mrv2_sheet$study_number <- as.character(mrv2_sheet$study_number)
      mrv2_sheet$Assignee <- as.character(mrv2_sheet$Assignee)
      mrv2_sheet$valid_latest_activity <- as.character(mrv2_sheet$valid_latest_activity)
      mrv2_sheet$presumptive_completion <- as.character(mrv2_sheet$presumptive_completion)
      mrv2_sheet$paid <- as.character(mrv2_sheet$paid)
      mrv2_sheet$amount <- as.character(mrv2_sheet$amount)
      
    }
    
  }
  #_____________________________________________________________________
  
  #It has dawned on me that data completed after the period under question may be the stopping eligible during period for
  #generate the eligible pending, date they became pending and the data  of the earliest close-eligible that is after the due date
  
  #________________________________________________________________________-
  #EVIDENCES
  
      #Surveillance evidence:
  surv_evidence_link <- evidence_related_links[(evidence_related_links$evidence_related_type == "surveillance_evidence"), "link"][[1]]
  
  hf_surv_evidence <-  range_read(surv_evidence_link,
                                 sheet = facility,
                                 range = NULL,
                                 col_names = TRUE,
                                 col_types = NULL,
                                 na = "",
                                 trim_ws = TRUE,
                                 skip = 0,
                                 #n_max = Inf,
                                 #guess_max = min(1000, n_max),
                                 .name_repair = "unique"
  )
  
  #hf_surv_evidence$photo_date <- as.Date(hf_surv_evidence$photo_date)
  hf_surv_evidence$photo_date <- as.Date(hf_surv_evidence$photo_date, format = "%d-%m-%Y")
  hf_surv_evidence <- hf_surv_evidence[!is.na(hf_surv_evidence$photo_date), c('study_number',
                                                                              'photo_date')]
  
  
  #Surveillance without evidence:
  surv_without_evidence_link <- evidence_related_links[(evidence_related_links$evidence_related_type == "surveillance_approved_without_evidence"), "link"][[1]]
  
  hf_surv_without_evidence <-  range_read(surv_without_evidence_link,
                             sheet = facility,
                             range = NULL,
                             col_names = TRUE,
                             col_types = NULL,
                             na = "",
                             trim_ws = TRUE,
                             skip = 0,
                             #n_max = Inf,
                             #guess_max = min(1000, n_max),
                             .name_repair = "unique"
  )
  
  hf_surv_without_evidence$`Contact Date` <- as.Date(hf_surv_without_evidence$`Contact Date`)
  hf_surv_without_evidence <- hf_surv_without_evidence[, c("study_number",
                                                           "Contact Date",
                                                           "Coordinator큦 Feedback")]
  
  
  #m18 evidence:
  m18_evidence_link <- evidence_related_links[(evidence_related_links$evidence_related_type == "m18_evidence"), "link"][[1]]
  
  hf_m18_evidence <-  range_read(m18_evidence_link,
                                  sheet = facility,
                                  range = NULL,
                                  col_names = TRUE,
                                  col_types = NULL,
                                  na = "",
                                  trim_ws = TRUE,
                                  skip = 0,
                                  #n_max = Inf,
                                  #guess_max = min(1000, n_max),
                                  .name_repair = "unique"
  )
  
  hf_m18_evidence <- hf_m18_evidence[, c("study_number",
                                         "completed yes/no")]
  
  
  #m18 without evidence:
  m18_without_evidence_link <- evidence_related_links[(evidence_related_links$evidence_related_type == "m18_approved_without_evidence"), "link"][[1]]
  
  hf_m18_without_evidence <-  range_read(m18_without_evidence_link,
                                          sheet = facility,
                                          range = NULL,
                                          col_names = TRUE,
                                          col_types = NULL,
                                          na = "",
                                          trim_ws = TRUE,
                                          skip = 0,
                                          #n_max = Inf,
                                          #guess_max = min(1000, n_max),
                                          .name_repair = "unique"
  )
  
  hf_m18_without_evidence <- hf_m18_without_evidence[, c('study_number',
                                                         'STATUS')]
  
  #___________________________________________________________

  #Generally eligible for the hf hitherto
  
  hf_period_surveillance_hitherto <- filter(eligible_surveillance_period_hitherto, hf == facility) #remove duplicate
  hf_period_surveillance_hitherto <- hf_period_surveillance_hitherto[order(factor(hf_period_surveillance_hitherto$instrument, levels = activity_order)), ]
  hf_period_surveillance_hitherto <- hf_period_surveillance_hitherto[!duplicated(hf_period_surveillance_hitherto$study_number), c('study_number',
                                                                                                                                  'instrument',
                                                                                                                                  'surv_eligible_date')]
  
  
  
  hf_period_mrv2_due_hitherto <- filter(eligible_period_mrv2_hitherto, hf == facility)
  hf_period_mrv2_due_hitherto$instrument <- 'mrv2_pending_alarm'
  hf_period_mrv2_due_hitherto <- hf_period_mrv2_due_hitherto[, c('study_number',
                                                                 'instrument',
                                                                 'bday_16_month')]
  
  hf_period_end_of_fu_due_hitherto <- filter(eligible_period_end_of_fu_hitherto, hf == facility)
  hf_period_end_of_fu_due_hitherto$instrument <- 'end_of_fu_pending_alarm'
  hf_period_end_of_fu_due_hitherto <- hf_period_end_of_fu_due_hitherto[, c('study_number',
                                                                           'instrument',
                                                                           'bday_18_month')]
  
  # eligible for the specific period in question 
  hf_specific_period_surveillance_hitherto <- filter(hf_period_surveillance_hitherto, 
                                              surv_eligible_date >= start_date &
                                                surv_eligible_date <= fu_reference_date
                                              )
  
  hf_specific_period_mrv2_due_hitherto <- filter(hf_period_mrv2_due_hitherto, 
                                             bday_16_month  >= start_date &
                                               bday_16_month  <= fu_reference_date
  )
  
  hf_specific_period_end_of_fu_due_hitherto <- filter(hf_period_end_of_fu_due_hitherto, 
                                             bday_18_month  >= start_date &
                                               bday_18_month  <= fu_reference_date
  )
  #__________________________________________________________________________
  
  #Generate pending alarms
  hf_recruitment_data <- filter(recruitment_data, hf == facility)
  
  hf_mrv2_pending_alarms <- alarm_fxn(hf_recruitment_data, "MRV2 Pending")#____________Pending
  hf_end_of_fu_alarms <- alarm_fxn(hf_recruitment_data, "END F/U Pending")#____________Pending

  hf_Surveillance_alarms <- alarm_fxn(hf_recruitment_data, "SURVEILLANCE") 
  
  hf_Surveillance_alarms_with_contact <- filter(hf_Surveillance_alarms, phone_contact != "NA_NA") #____________Pending
  hf_Surveillance_alarms_with_contact <- merge(hf_Surveillance_alarms_with_contact,
                                               next_visit_frame)
  hf_Surveillance_alarms_with_contact <- select(hf_Surveillance_alarms_with_contact, -'hf')
  
  hf_Surveillance_alarms_without_contact <- filter(hf_Surveillance_alarms, phone_contact == "NA_NA")
  
  #for surv_house those handed over from the call attempts also make part of the data frame
  hand_over_still_pending <- data.frame()
  
  if(nrow(gs_surveillance_calls) > 0){
    to_hand_over <- gs_surveillance_calls[tolower(gs_surveillance_calls$hand_over) =="yes" &
                                            !is.na(gs_surveillance_calls$hand_over), 
                                          c('study_number',
                                            'call_details_if_not_successful')]
    if(nrow(to_hand_over) > 0){
      pending_to_hand_over <- to_hand_over[(to_hand_over$study_number %in% hf_Surveillance_alarms$study_number), ]
      hand_over_still_pending <- rbind(hand_over_still_pending,
                                       pending_to_hand_over)
    }
  }
  
  hf_house_surv <- bind_rows(hf_Surveillance_alarms_without_contact,   
                              hand_over_still_pending)
  hf_house_surv <-merge(hf_house_surv,
                        next_visit_frame)
  hf_house_surv <- select(hf_house_surv, -c('hf','phone_contact')) #____________Pending
  
  #write.csv(hf_house_surv, paste(facility, '_hf_house_surv.csv'), row.names = F)
    #however, it is vital to ensure that they are still part of the pending surv - check that list versus hf_Surveillance_alarms_with_contact
  
  #______________________________________________________________________
  #Generate COMPLETED data for not-yet-exited participants for
  
  #hf_field_data <- filter(field_data_identified_exited_removed, hf == facility)
  #hf_field_data_grouped <- group_by(hf_field_data, study_number)
  #hf_field_data_latest <- filter(hf_field_data_grouped, event_date == max(event_date, na.rm = T))
  #hf_period_field_data <- filter(hf_field_data, (event_date >= start_date & event_date <= fu_reference_date))
  
  hf_potential_period_related_data <- filter(potential_period_related_data, hf == facility)
  
  hf_potential_period_related_data <- code_to_value(hf_potential_period_related_data, 'ms_contact_caretaker', surv_contact_type, 1)
  
  hf_potential_period_related_data <- code_to_value(hf_potential_period_related_data, 'ms_status_child', surv_fu_status, 1)
  hf_potential_period_related_data[hf_potential_period_related_data$ms_status_child == 88 & 
                                     !is.na(hf_potential_period_related_data$ms_status_child) , "ms_status_child"] <- "Unknown"
  
  hf_potential_period_related_data <- code_to_value(hf_potential_period_related_data, 'hh_child_seen', child_seen, 0)
  hf_potential_period_related_data <- code_to_value(hf_potential_period_related_data, 'fu_type', m18_contact_type, 1)
  hf_potential_period_related_data <- code_to_value(hf_potential_period_related_data, 'phone_child_status', m18_call_status, 1)
  hf_potential_period_related_data <- code_to_value(hf_potential_period_related_data, 'reachable_status', m18_house_status, 1)
  
  
  hf_potential_period_completed <- hf_potential_period_related_data[, c('study_number',
                                                              'instrument',
                                                              'event_date',
                                                              'ms_contact_caretaker',
                                                              'ms_status_child',
                                                              'fu_type',
                                                              'phone_child_status',
                                                              'reachable_status',
                                                              'hh_child_seen',
                                                              'interviewer')]
  
  #Generating those complete within the period in question
  
    #surv calls
  
  hf_surv_call_completed_within_period <- filter(hf_potential_period_related_data, (ms_contact_caretaker == 'Phone Call' &
                                                                                      event_date <= fu_reference_date))[,  c('study_number',
                                                                                                                             'instrument',
                                                                                                                             'event_date',
                                                                                                                             'interviewer',
                                                                                                                             'ms_contact_caretaker',
                                                                                                                             'ms_status_child'
                                                                                                                             )
                                                                                                                        ]
                                                                        
  
  colnames(hf_surv_call_completed_within_period) <- c('study_number',
                                                      'valid_latest_activity',
                                                      'valid_latest_activity_date',
                                                      'interviewer_id',
                                                      'contact_type',
                                                      'status')
  
  #hf_surv_call_completed_within_period <- select(hf_surv_call_completed_within_period, -'ms_contact_caretaker')
  
    
  #surv house
  
  
  hf_surv_house_within_completed <- filter(hf_potential_period_related_data, (ms_contact_caretaker == 'Household visit' &
                                                                                event_date <= fu_reference_date))[ ,c('study_number',
                                                                                                                      'instrument',
                                                                                                                      'event_date',
                                                                                                                      'interviewer',
                                                                                                                      'ms_contact_caretaker',
                                                                                                                      'ms_status_child'
                                                                                                                      )]
  
  colnames(hf_surv_house_within_completed) <- c('study_number',
                                                      'valid_latest_activity',
                                                      'valid_latest_activity_date',
                                                      'interviewer_id',
                                                      'contact_type',
                                                      'status')
  
  #hf_surv_house_within_completed <- select(hf_surv_house_within_completed, -'ms_contact_caretaker')
  
    #M18
  
  hf_m18_completed_within_period <- filter(hf_potential_period_related_data, (instrument == 'M18' &
                                                                                event_date <= fu_reference_date))[,  c('study_number',
                                                                                                                       'instrument',
                                                                                                                       'event_date',
                                                                                                                       'interviewer',
                                                                                                                       'fu_type',
                                                                                                                       'phone_child_status',
                                                                                                                       'hh_child_seen',
                                                                                                                       'reachable_status'
                                                                                                                       )
                                                                                                                  ]
  
  hf_m18_completed_within_period[!is.na(hf_m18_completed_within_period$phone_child_status), "status"] <- hf_m18_completed_within_period[!is.na(hf_m18_completed_within_period$phone_child_status), "phone_child_status"]
  
  hf_m18_completed_within_period[hf_m18_completed_within_period$hh_child_seen == "seen" & !is.na(hf_m18_completed_within_period$hh_child_seen), "status"] <- 
    hf_m18_completed_within_period[hf_m18_completed_within_period$hh_child_seen == "seen"& !is.na(hf_m18_completed_within_period$hh_child_seen), "hh_child_seen"]
  
  
  hf_m18_completed_within_period[!is.na(hf_m18_completed_within_period$reachable_status), "status"] <- hf_m18_completed_within_period[!is.na(hf_m18_completed_within_period$reachable_status), "reachable_status"]
  
  
  hf_m18_completed_within_period <- select(hf_m18_completed_within_period, -c('phone_child_status',
                                                                              'hh_child_seen',
                                                                              'reachable_status'))
  colnames(hf_m18_completed_within_period) <- c('study_number',
                                                      'valid_latest_activity',
                                                      'valid_latest_activity_date',
                                                      'interviewer_id',
                                                      'contact_type',
                                                      'status')
  
    #MRV2
  hf_mrv2_completed_within_period <- filter(hf_potential_period_related_data, (instrument == 'MRV2' &
                                                                                event_date <= fu_reference_date))[,  c('study_number',
                                                                                                                       'instrument',
                                                                                                                       'event_date'
                                                                                                                       )
                                                                                                                  ]
  colnames(hf_mrv2_completed_within_period) <- c('study_number',
                                                'valid_latest_activity',
                                                'valid_latest_activity_date')
  
  

  'ms_contact_caretaker'
  'ms_status_child'
  'fu_type'
  'phone_child_status'
  'reachable_status'
  
  #Stopping conditions#
  
  #Surveillance 
    #mrv2 now pending
    #M18 now pending
    #All others after eligible date
  
  #Mrv2
    #M18 alarm / m18 done
    #Mrv2 completed
    #Death
    #Wdrawal
  
  #M18
    #M18 completed
    #Death
    #Wdrawal
  
  #putting all the posible stopping conditions in one frame
  
  #_______________________________________________________________________
  # use the instrument column for consistency
  # We already have the frame for actual data completed
  #No we generate the frame for the alarms that pop up after the start date
  
  hf_potential_period_mrv2_stopper <- filter(potential_period_mrv2_stopper, hf == facility)
  hf_potential_period_mrv2_stopper$instrument <- 'mrv2_pending_alarm'
  hf_potential_period_mrv2_stopper <- hf_potential_period_mrv2_stopper[, c('study_number',
                                                                           'instrument',
                                                                           'bday_15_month')]
  
  hf_potential_period_mrv2_stopper <- filter(hf_potential_period_mrv2_stopper, bday_15_month <= today())
  
  
  
  
  hf_potential_period_m18_stopper <- filter(potential_period_m18_stopper, hf == facility)
  hf_potential_period_m18_stopper$instrument <- 'end_of_fu_pending_alarm'
  hf_potential_period_m18_stopper <- hf_potential_period_m18_stopper[, c('study_number',
                                                                         'instrument',
                                                                         'bday_18_month')]
  
  hf_potential_period_m18_stopper <- filter(hf_potential_period_m18_stopper, bday_18_month <= today())
  
  
  
  
  universal_stopping_frame <- bind_rows(hf_potential_period_completed,
                                        hf_potential_period_mrv2_stopper,
                                        hf_potential_period_m18_stopper
                                        )
  
  #universal_stopping_frame <- universal_stopping_frame[order(factor(universal_stopping_frame$instrument, levels = activity_order)),]
  
  #combining event and follow_up types
  
  universal_stopping_frame[!is.na(universal_stopping_frame$bday_15_month), "event_date"] <- universal_stopping_frame[!is.na(universal_stopping_frame$bday_15_month), "bday_15_month"]
  universal_stopping_frame[!is.na(universal_stopping_frame$bday_18_month), "event_date"] <- universal_stopping_frame[!is.na(universal_stopping_frame$bday_18_month), "bday_18_month"]
  
  
  universal_stopping_frame[!is.na(universal_stopping_frame$ms_contact_caretaker), "contact_type"] <- universal_stopping_frame[!is.na(universal_stopping_frame$ms_contact_caretaker), "ms_contact_caretaker"]
  universal_stopping_frame[!is.na(universal_stopping_frame$fu_type), "contact_type"] <- universal_stopping_frame[!is.na(universal_stopping_frame$fu_type), "fu_type"]
  
  universal_stopping_frame[!is.na(universal_stopping_frame$phone_child_status), "status"] <- universal_stopping_frame[!is.na(universal_stopping_frame$phone_child_status), "phone_child_status"]
  universal_stopping_frame[universal_stopping_frame$hh_child_seen == "seen" & !is.na(universal_stopping_frame$hh_child_seen), "status"] <- universal_stopping_frame[universal_stopping_frame$hh_child_seen == "seen" &!is.na(universal_stopping_frame$hh_child_seen), "hh_child_seen"]
  universal_stopping_frame[!is.na(universal_stopping_frame$ms_status_child), "status"] <- universal_stopping_frame[!is.na(universal_stopping_frame$ms_status_child), "ms_status_child"]
  universal_stopping_frame[!is.na(universal_stopping_frame$reachable_status), "status"] <- universal_stopping_frame[!is.na(universal_stopping_frame$reachable_status), "reachable_status"]
  
  universal_stopping_frame <- select(universal_stopping_frame, -c("ms_contact_caretaker",
                                              "ms_status_child",
                                              "fu_type",
                                              "phone_child_status",
                                              'hh_child_seen',
                                              "reachable_status",
                                              "bday_15_month",
                                              "bday_18_month"))
  names(universal_stopping_frame) <- c('study_number',
                             'valid_latest_activity',
                             'valid_latest_activity_date',
                             'interviewer_id',
                             'contact_type',
                             'status')

  
  surveillance_stopping_frame <- universal_stopping_frame
  
  mrv2_stoppers <- c('MRV2',
                     "end_of_fu_pending_alarm",
                     'M18',
                     "death",
                     "wdrawal")
  mrv2_stopping_frame <- universal_stopping_frame[universal_stopping_frame$valid_latest_activity %in% mrv2_stoppers & !is.na(universal_stopping_frame$valid_latest_activity), ]
 
  m18_stoppers <- c('M18',
                     "death",
                     "wdrawal")
  m18_stopping_frame <- universal_stopping_frame[universal_stopping_frame$valid_latest_activity %in% m18_stoppers & !is.na(universal_stopping_frame$valid_latest_activity), ]
  #MRV2_stopper_filter <-m18_stoppers
  
  
  
  
  
  
  
  #surv_house <- filter(surv_house, surv_eligible_date >= start_date)
  
  #write.csv(surv_house, paste(facility, "_surv_house.csv"), row.names = F)
  
  m18 <- icaria_fu_tracker_function(gs_M18,
                                     M18_sheet,
                                     'valid_latest_activity',
                                     hf_m18_completed_within_period,
                                     'valid_latest_activity_date',
                                     hf_end_of_fu_alarms,
                                     #activities,
                                     hf_period_end_of_fu_due_hitherto,
                                     hf_specific_period_end_of_fu_due_hitherto,
                                     'bday_18_month',
                                     m18_stopping_frame,
                                     m18_relevant_stopper_fields)
  
  
    verification_m18 <- left_join(m18,
                            hf_m18_evidence)
  
  verification_m18 <- left_join(verification_m18,
                            hf_m18_without_evidence)
  
  
  #verification_18$evidence_date_difference <- interval(verification$valid_latest_activity_date, verification$photo_date)  %/% days()
  #verification$coordinator_approval_date_difference <- interval(verification$valid_latest_activity_date, verification$`Contact Date`)  %/% days()
  
  filter_evidence_m18 <- verification_m18$valid_latest_activity == "M18" &
    !is.na(verification_m18$valid_latest_activity) &
    verification_m18$contact_type == 'Household visit' &
    !is.na(verification_m18$contact_type) &
    verification_m18$`completed yes/no` == 'yes' &
    !is.na(verification_m18$`completed yes/no`)
  
  #test <- verification_m18[filter_evidence, "supported_by_evidence?"]# <- "yes"
  verification_m18[filter_evidence_m18, 'supported_by_evidence?'] <- "yes"
  
  
  filter_without_evidence_m18_1 <- verification_m18$valid_latest_activity == "M18" &
    !is.na(verification_m18$valid_latest_activity) &
    verification_m18$contact_type == 'Household visit' &
    !is.na(verification_m18$contact_type) &
    verification_m18$`STATUS` == "REACHABLE" &
    !is.na(verification_m18$`STATUS`)
  
  
  filter_without_evidence_m18_2 <- verification_m18$valid_latest_activity == "M18" &
    !is.na(verification_m18$valid_latest_activity) &
    verification_m18$contact_type == 'Household visit' &
    !is.na(verification_m18$contact_type) &
    verification_m18$`STATUS` == "UNREACHABLE" &
    !is.na(verification_m18$`STATUS`)
  
  filter_without_evidence_m18_3 <- verification_m18$valid_latest_activity == "M18" &
    !is.na(verification_m18$valid_latest_activity) &
    verification_m18$contact_type == 'Household visit' &
    !is.na(verification_m18$contact_type) &
    !is.na(verification_m18$`STATUS`) &
    verification_m18$`STATUS` == "PENDING"
             
             
  
  
  verification_m18[filter_without_evidence_m18_1, "approved_by_coordinator"] <- "Approved by Coordinator"
  verification_m18[filter_without_evidence_m18_2, "approved_by_coordinator"] <- "UNREACHABLE"
  verification_m18[filter_without_evidence_m18_3, "approved_by_coordinator"] <- "Awaiting Coordinator's response"
  
  #remember to include calls sheet
  
  
  verification_m18[(verification_m18$`supported_by_evidence?` == "yes" & !is.na(verification_m18$`supported_by_evidence?`))|
                 (verification_m18$approved_by_coordinator =="Approved by Coordinator" & !is.na(verification_m18$approved_by_coordinator)), "approved_by_data"] <- "Yes"
  
  #Attempt to remove duplicates for vefified and pending verification_m18
  
  #verifiables
  
  verification_m18_first <- verification_m18[!is.na(verification_m18$`supported_by_evidence?`) |
                                       !is.na(verification_m18$STATUS), ]
  
  verification_m18_first <- verification_m18_first[!duplicated(verification_m18_first$study_number), ]
  
  #Pending evidence whatsoever
  no_verification_m18 <- verification_m18[is.na(verification_m18$`supported_by_evidence?`) &
                                    is.na(verification_m18$STATUS), ]
  
  no_verification_m18 <- no_verification_m18[!(no_verification_m18$study_number %in%verification_m18_first$study_number) &
                                       !duplicated(no_verification_m18$study_number), ]
  
  m18_to_google <- rbind(verification_m18_first,
                         no_verification_m18)
  
  
  
  
  m18_ <- m18_to_google[, colnames(m18)]
  
  m18_ <- m18_[order(factor(m18_$valid_latest_activity, levels = activity_order)),]
  

  m_18_cumulative <- rbind(m_18_cumulative,  #combining all m18s)
                          m18_)
  
  #bringing out those without evidence:
  m18_without_data_approval_filter <- is.na(m18_$approved_by_data) &
    !is.na(m18_$valid_latest_activity) &
    m18_$valid_latest_activity == "M18" &
    m18_$contact_type == "Household visit"
  
  m18_without_data_approval <- m18_[m18_without_data_approval_filter, ]
  if(nrow(m18_without_data_approval) > 0){
    write.csv(m18_without_data_approval, paste0(facility,"_",period_under_consideration,"_", 'm18_without_data_approval.csv'), row.names = F)
  }
  
  
  
  range_write(hf_link,
              m18_,
              sheet = "M18",
              range = "M18",
              col_names = T,
              reformat = F
  )
  
  range_clear(hf_link,
              sheet = "M18",
              range = cell_rows((nrow(m18_) + 2):1000),
              reformat = F
              #shift = NULL
  )
  
  
  
  mrv2 <- icaria_fu_tracker_function(gs_MRV2,
                                     mrv2_sheet,
                                     'valid_latest_activity',
                                     hf_mrv2_completed_within_period,
                                     'valid_latest_activity_date',
                                     hf_mrv2_pending_alarms,
                                     #activities,
                                     hf_period_mrv2_due_hitherto,
                                     hf_specific_period_mrv2_due_hitherto,
                                     'bday_16_month',
                                     mrv2_stopping_frame,
                                     mrv2_relevant_stopper_fields)
  
  mrv2 <- mrv2[order(factor(mrv2$valid_latest_activity, levels = activity_order)),]
  
  range_write(hf_link,
              mrv2,
              sheet = "MRV2",
              range = "MRV2",
              col_names = T,
              reformat = F
  )
  
  range_clear(hf_link,
              sheet = "MRV2",
              range = cell_rows((nrow(mrv2) + 2):1000),
              reformat = F
              #shift = NULL
  )
  
  
 

  surv_calls <- icaria_fu_tracker_function(gs_surveillance_calls,
                                           surv_calls_sheet,
                                           'valid_latest_activity',
                                           hf_surv_call_completed_within_period,
                                           'valid_latest_activity_date',
                                           hf_Surveillance_alarms_with_contact,
                                           #activities,
                                           hf_period_surveillance_hitherto,
                                           hf_specific_period_surveillance_hitherto,
                                           'surv_eligible_date',
                                           surveillance_stopping_frame,
                                           surv_relevant_stopper_fields,
                                           data_after_start_date = data_completed_after_start_date
                                           )
  
  #surv_calls <- filter(surv_calls, surv_eligible_date >= start_date)
  surv_calls_dups <- surv_calls[duplicated(surv_calls$study_number), ]
        if(nrow(surv_calls_dups) > 0){
          print(paste("DUPLICATES IN CALLS OF__________________", facility))
        }
  
  surv_calls <- merge(surv_calls,
                      contacts,
                      sort = FALSE)
  
  surv_calls <- surv_calls[order(factor(surv_calls$valid_latest_activity, levels = activity_order)),]
  
  range_write(hf_link,
              surv_calls,
              sheet = "Surveillance_Calls",
              range = "Surveillance_Calls",
              col_names = T,
              reformat = F
  )
  
  range_clear(hf_link,
              sheet = "Surveillance_Calls",
              range = cell_rows((nrow(surv_calls) + 2):1000),
              reformat = F
              #shift = NULL
  )
  
  
  #Finding errors and long pendings
  long_pending <- filter(surv_calls, ((tolower(call_successful) != "yes"| is.na(call_successful))  & (tolower(hand_over) != "yes" | is.na(hand_over)) & is.na(valid_latest_activity)))[, c("study_number", "due_date")]
  
  if(nrow(long_pending) > 0){
    write.csv(long_pending, paste(period_under_consideration,"_", facility, "_please focus on.csv"), row.names = F)
  }
  
  should_be_entered <- filter(surv_calls, (tolower(call_successful) == "yes" &is.na(valid_latest_activity)))[, c("study_number", "due_date", 'Assignee', "call_successful",'hand_over', "valid_latest_activity")]
  if(nrow(should_be_entered) >  0){
    write.csv(should_be_entered, paste(period_under_consideration,"_", facility, "_why_not_entered.csv"), row.names = F)
  }
  
  
  #write.csv(surv_calls, paste(facility, "_surv_calls.csv"), row.names = F)
  #
  
  surv_house <- icaria_fu_tracker_function(gs_surveillance_house,
                                           surv_house_sheet,
                                           'valid_latest_activity',
                                           hf_surv_house_within_completed,
                                           'valid_latest_activity_date',
                                           hf_house_surv,
                                           #activities,
                                           hf_period_surveillance_hitherto,
                                           hf_specific_period_surveillance_hitherto,
                                           'surv_eligible_date',
                                           surveillance_stopping_frame,
                                           surv_relevant_stopper_fields,
                                           data_after_start_date = data_completed_after_start_date)
  
  
  #surv_calls <- merge(surv_calls,
   #                   contacts)
  #write.csv(surv_calls, paste(facility, "_surv_calls.csv"), row.names = F)
  
  surv_house_dups <- surv_house[duplicated(surv_house$study_number), ]
  if(nrow(surv_house_dups) > 0){
    print(paste("DUPLICATES IN HOUSE OF__________________", facility))
  }
  
  verification <- left_join(surv_house,
                          hf_surv_evidence)
  
  verification <- left_join(verification,
                          hf_surv_without_evidence)
  
 
  verification$evidence_date_difference <- interval(verification$valid_latest_activity_date, verification$photo_date)  %/% days()
  verification$coordinator_approval_date_difference <- interval(verification$valid_latest_activity_date, verification$`Contact Date`)  %/% days()
  
  filter_evidence <- verification$valid_latest_activity == "M_Surv" &
    !is.na(verification$valid_latest_activity) &
    verification$contact_type == 'Household visit' &
    !is.na(verification$contact_type) &
    !is.na(verification$evidence_date_difference)&
    verification$evidence_date_difference >= -2 &
    verification$evidence_date_difference <= 46
  
  #test <- verification[filter_evidence, "supported_by_evidence?"]# <- "yes"
  verification[filter_evidence, 'supported_by_evidence?'] <- "yes"
  
  
  filter_without_evidence1 <- verification$valid_latest_activity == "M_Surv" &
    !is.na(verification$valid_latest_activity) &
    verification$contact_type == 'Household visit' &
    !is.na(verification$contact_type) &
    !is.na(verification$coordinator_approval_date_difference)&
    verification$coordinator_approval_date_difference >= -2 &
    verification$coordinator_approval_date_difference <= 2 &
    verification$`Coordinator큦 Feedback` == "Approved" &
    !is.na(verification$`Coordinator큦 Feedback`)
  
  
  filter_without_evidence2 <- verification$valid_latest_activity == "M_Surv" &
    !is.na(verification$valid_latest_activity) &
    verification$contact_type == 'Household visit' &
    !is.na(verification$contact_type) &
    !is.na(verification$coordinator_approval_date_difference)&
    verification$coordinator_approval_date_difference >= -2 &
    verification$coordinator_approval_date_difference <= 2 &
     verification$`Coordinator큦 Feedback` == "Pending" &
    !is.na(verification$`Coordinator큦 Feedback`)
  
  filter_without_evidence3 <- verification$valid_latest_activity == "M_Surv" &
    !is.na(verification$valid_latest_activity) &
    verification$contact_type == 'Household visit' &
    !is.na(verification$contact_type) &
    !is.na(verification$coordinator_approval_date_difference)&
    verification$coordinator_approval_date_difference >= -2 &
    verification$coordinator_approval_date_difference <= 2 &
    is.na(verification$`Coordinator큦 Feedback`)
  
  
  verification[filter_without_evidence1, "approved_by_coordinator"] <- "Approved by Coordinator"
  verification[filter_without_evidence2, "approved_by_coordinator"] <- "Pending by Coordinator"
  verification[filter_without_evidence3, "approved_by_coordinator"] <- "Awaiting Coordinator's response"
  
  
  verification[(verification$`supported_by_evidence?` == "yes" & !is.na(verification$`supported_by_evidence?`))|
                 (verification$approved_by_coordinator =="Approved by Coordinator" & !is.na(verification$approved_by_coordinator)), "approved_by_data"] <- "Yes"
  
  #Attempt to remove duplicates for vefified and pending verification
  
    #verifiables
  
  verification_first <- verification[!is.na(verification$`supported_by_evidence?`) |
                                       !is.na(verification$`Coordinator큦 Feedback`), ]
  
  verification_first <- verification_first[!duplicated(verification_first$study_number), ]
  
  #Pending evidence whatsoever
  no_verification <- verification[is.na(verification$`supported_by_evidence?`) &
                                    is.na(verification$`Coordinator큦 Feedback`), ]
  
  no_verification <- no_verification[!(no_verification$study_number %in%verification_first$study_number) &
                                       !duplicated(no_verification$study_number), ]
  
  surv_house_to_google <- rbind(verification_first,
                                no_verification)
  
  surv_house_to_google <- surv_house_to_google[, colnames(surv_house)]
  
  #bringing out those without evidence:
  
  surv_house_to_google_without_data_approval_filter <- is.na(surv_house_to_google$approved_by_data) &
    !is.na(surv_house_to_google$valid_latest_activity) &
    surv_house_to_google$valid_latest_activity == "M_Surv" &
    surv_house_to_google$contact_type == "Household visit"
  
  surv_house_to_google_without_data_approval <- surv_house_to_google[surv_house_to_google_without_data_approval_filter, ]
  
 if(nrow(surv_house_to_google_without_data_approval) > 0){
   write.csv(surv_house_to_google_without_data_approval, paste0(facility,"_",period_under_consideration,"_", 'surv_house_without_data_approval.csv'), row.names = F)
 } 
  
  surv_house_to_google <- surv_house_to_google[order(factor(surv_house_to_google$valid_latest_activity, levels = activity_order)),]
  
   
  range_write(hf_link,
              surv_house_to_google,
              sheet = "Surveillance_House",
              range = "Surveillance_House",
              col_names = T,
              reformat = F
              )
  
  range_clear(hf_link,
              sheet = "Surveillance_House",
              range = cell_rows((nrow(surv_house_to_google) + 2):1000),
              reformat = F
              #shift = NULL
  )
  
}
