library(redcapAPI)
library(dplyr)
library(tidyr)
library(lubridate)
library(googlesheets4)
library(googledrive)
library(tidyverse)

source("../icaria_project_tokens.R")



source("contacts.R")

contacts <- read.csv("contacts.csv")



write.csv(contacts, "test/contacts.csv", row.names = F)

hfs <- c("HF01","HF02","HF03","HF04","HF05","HF06","HF08","HF10","HF11","HF12","HF13","HF15","HF16","HF17")

# PART2: Export from all health facilities
my.fields <- c('record_id',
               'fu_type',
               'phone_success',
               'child_fu_status',
               'study_number',
               'hh_child_seen',
               'hh_why_not_child_seen',
               'phone_child_status',
               'hh_interviewer_id',
               'hh_date',
               'int_date'
               )


my.epi.events <- c('epipenta1_v0_recru_arm_1',
                   'epimvr2_v6_iptisp6_arm_1',
                   'hhat_18th_month_of_arm_1'
                   )

# Exporting data from REDCap
data <- data.frame()
for (hf in names(kRedcapTokens)) {
  print(paste("Extracting data from", hf))
  
  rcon <- redcapConnection(kRedcapAPIURL, kRedcapTokens[[hf]])
  hf.data <- exportRecords(
    rcon,
    factors            = F,
    labels             = F,
    fields             = my.fields,
    events             = my.epi.events
  )
  # cbind is attaching by columns so we use it to attach a new column with the 
  # HF code
  #Create PK column
  hf.data$pk <- paste(hf, hf.data$record_id, sep = "_")
  
  if ((substr(hf, 1, 2)  == "HF") & (substr(hf, 5, 5) == ".")) {
    hf <- substr(hf, 1, 4)
  } else {
    hf <-  hf
  }
  
  hf.data <- cbind(hf = hf, hf.data)
  
  # rbind is attaching by rows so we use it to attach the individual HF data 
  # frames together
  data <- rbind(data, hf.data)
}

#Getting MRV2 completed

filter <- data$redcap_event_name == "epimvr2_v6_iptisp6_arm_1" & !is.na(data$int_date)
mrv2_completed <- data[filter, ]

#Getting completed participants

data$child_fu_status <- substr(data$child_fu_status , 1,9)

filter <- data$child_fu_status == "COMPLETED" & !is.na(data$child_fu_status)
completed_participants <- data[filter, c("study_number", "child_fu_status", 'pk')]

#Getting status when completed

filter <- data$redcap_event_name == "hhat_18th_month_of_arm_1"
last_visit <- data[filter, ]

#Getting the latest instance completed
last_visit <- group_by(last_visit, pk)
last_visit <- filter(last_visit, hh_date == max(hh_date, na.rm = T))

#--------
amt_data <- data.frame()
amt <- function(type){
  type <- filter(hf_visit, fu_type == type)
  type_amount <- nrow(type)
  return(type_amount)
}
for(HF in hfs){
  hf_visit <- filter(last_visit, hf == HF)
  phone <- amt(1)
  home <- amt(2)
  facility <- amt(3)
  hf_dat <- data.frame(HF,phone, home, facility)
  amt_data <- rbind(amt_data, hf_dat)
}
write.csv(amt_data, "m18_fu_type.csv", row.names = F)
ch


#--------

#Home visits

filter <- last_visit$fu_type == 2 & !is.na(last_visit$fu_type)
home_visit <- last_visit[filter, ]

  #seen children
filter <- home_visit$hh_child_seen == 1 & !is.na(home_visit$hh_child_seen)
seen_children <- home_visit[filter, ]
seen_children$status <- "seen" 

  #Not seen
filter <- home_visit$hh_child_seen == 0 & !is.na(home_visit$hh_child_seen)
children_not_seen <- home_visit[filter, ]
children_not_seen$status <- "not seen" 

  #Error in home visits
filter <- is.na(home_visit$hh_child_seen)
error_home_visit <- home_visit[filter, ]

  #Combine seen and not seen

house_visit <- rbind(seen_children, children_not_seen)  

#Successful phone calls

filter <- last_visit$fu_type == 1 & !is.na(last_visit$fu_type) & last_visit$phone_success == 1
successful_phone_calls <- last_visit[filter, ]

  #error_home_visits
filter <- is.na(successful_phone_calls$phone_child_status)
error_phone_calls <- successful_phone_calls[filter, ]

#Calls
calls <- data.frame()

  # Alive during phone calls
filter <- successful_phone_calls$phone_child_status == "1" & !is.na(successful_phone_calls$phone_child_status)
alive <- successful_phone_calls[filter, ]

if (nrow(alive) > 0){
  alive$status <- "Alive"
  calls <- rbind(calls, alive)
}

  #Dead
filter <- successful_phone_calls$phone_child_status == "2" & !is.na(successful_phone_calls$phone_child_status)
dead <- successful_phone_calls[filter, ]

if (nrow(dead) > 0){
  dead$status <- "Dead"
  calls <- rbind(calls, dead)
}

  #Admitted to hospital
filter <- successful_phone_calls$phone_child_status == "3" & !is.na(successful_phone_calls$phone_child_status)
admitted <- successful_phone_calls[filter, ]

if (nrow(admitted) > 0){
  admitted$status <- "Admitted to hospital"
  calls <- rbind(calls, admitted)
}

  #Migrated
filter <- successful_phone_calls$phone_child_status == "4" & !is.na(successful_phone_calls$phone_child_status)
migrated <- successful_phone_calls[filter, ]

if (nrow(admitted) > 0){
  migrated$status <- "Migrated"
  calls <- rbind(calls, migrated)
}


#Put calls and home visits together
last_follow_up <- rbind(house_visit, calls)
last_follow_up_data <- last_follow_up[, c("hf", "pk", "status", "hh_date","hh_interviewer_id")]
last_follow_up_data$hh_date <- as.Date(last_follow_up_data$hh_date)

#Merge completed participants with last hhfu data

completed_visits <- merge(completed_participants, last_follow_up_data)
completed_visits <- completed_visits[, c("hf",
                                         "study_number",
                                         "child_fu_status",
                                         "status",
                                         "hh_date",
                                         "hh_interviewer_id",
                                         "pk"
                                         )
                                     ]

#Getting home visits from completed set
filter <- completed_visits$status == "seen"
completed_at_home <- completed_visits[filter, ]
#completed_at_home <- data.frame()

gs4_auth()
1

verification_sheet <- gs4_get("https://docs.google.com/spreadsheets/d/1RyDIgqKG8mC5ukEYS9KiUkZ8HYMsVbHR_UmNGLJB3Os")
verified_pictures <-gs4_get("https://docs.google.com/spreadsheets/d/1bJgsc-Bw8L1fZuaEQ5rn3i0mQm_ZZ2uoSGquQVl3SWQ")

for(hf in hfs){
    filter <- completed_at_home$hf == hf
    hf_completed_at_home <- completed_at_home[filter, c("study_number",
                                                        "hh_date",
                                                        "hh_interviewer_id"
                                                        )
                                              ]
    
    hf_completed_at_home$`verified(yes/no)` <- ""
    hf_completed_at_home$type_of_verification_contact <- ""
    hf_completed_at_home$`verified_stamped_U5_card?(yes/no)` <- ""
    hf_completed_at_home$`verified_stamped_icaria_id_card?(yes/no)` <- ""  
    hf_completed_at_home$comment <- ""
    hf_completed_at_home$data_team_validation <- ""
    hf_completed_at_home$pending_validation_reason <- ""
    
    hf_gs_verification <- range_read( verification_sheet,
                                      sheet = hf,
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
    
    #REDCAP Data not already in the sheet
    
    filter <-   !(hf_completed_at_home$study_number %in% hf_gs_verification$study_number)
    add_to_gs <- hf_completed_at_home[filter, ]
    
    #Also check validation for those coming directly from redcap
    #filter <- add_to_gs$study_number %in% validated_photos$study_number
    #redcap_valid <- add_to_gs[filter, ]
    #redcap_valid$data_team_validation <- "Validated"
    #redcap_valid$pending_validation_reason <- "NA"
    #validated <- nrow(redcap_valid)
    
    #redcap_pending_validity <- add_to_gs[!filter, ]
    #redcap_pending_validity$data_team_validation <- "Pending Validation"
    #redcap_pending_validity$pending_validation_reason <- ""
    # pending_validation <- nrow(redcap_pending_validity)
    
    #redcap_valid_and_invalid <- rbind(redcap_valid, redcap_pending_validity)
    
    to_google <- rbind(hf_gs_verification,
                       add_to_gs
                       )
    
    range_write(verification_sheet,
                to_google,
                sheet = hf,
                range = hf,
                col_names = T,
                reformat = F
                )
}



validation_list <- data.frame()
hf_validation <- data.frame() 
hfs <- c("HF01","HF02","HF03","HF04","HF05","HF06","HF08","HF10","HF11","HF12","HF13","HF15","HF16","HF17")

gs4_auth()
1

verification_sheet <- gs4_get("https://docs.google.com/spreadsheets/d/1RyDIgqKG8mC5ukEYS9KiUkZ8HYMsVbHR_UmNGLJB3Os")
verified_pictures <-gs4_get("https://docs.google.com/spreadsheets/d/1bJgsc-Bw8L1fZuaEQ5rn3i0mQm_ZZ2uoSGquQVl3SWQ")

for(hf in hfs){
  
  hf_gs_verification <- range_read( verification_sheet,
                                    sheet = hf,
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
  
  hf_gs_photo <- range_read( verified_pictures,
                             sheet = hf,
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
  
  #Validation requires checking whether the record is evidenced in the photo sheet
  filter <- hf_gs_photo$`completed yes/no` == "yes" & !is.na(hf_gs_photo$`completed yes/no`)
  validated_photos <- hf_gs_photo[filter, ]
  
  #Verified by phone call - no evidence needed
  filter <- hf_gs_verification$type_of_verification_contact == "phone call" &
    !is.na(hf_gs_verification$type_of_verification_contact)
  phone_call_verification <- hf_gs_verification[filter, ]
  phone_call_verification$data_team_validation <- "c/o_supervisor"
  phone_call_verification$pending_validation_reason <- "NA"
  phone_verification <- nrow(phone_call_verification)
  
  # Verified by home visits
  filter <- hf_gs_verification$type_of_verification_contact == "home visit" &
    !is.na(hf_gs_verification$type_of_verification_contact)
  home_visit_verification <- hf_gs_verification[filter, ]
  home_verification <- nrow(home_visit_verification)
  
  #Home visits with evidence
  filter <- home_visit_verification$study_number %in% validated_photos$study_number
  home_visit_with_evidence <- home_visit_verification[filter, ]
  home_visit_with_evidence$data_team_validation <- "home visit with evidence"
  home_visit_with_evidence$pending_validation_reason <- "NA"
  
  #Home visits with no evidence
  home_visit_without_evidence <- home_visit_verification[!filter, ]
  home_visit_without_evidence$data_team_validation <- "home visit without evidence"
  
    #Facility visit and fieldworker provided evidence both characterized as evidence based
  filter <- hf_gs_verification$type_of_verification_contact == "FW provided evidence"|
    hf_gs_verification$type_of_verification_contact == "facility visit" | 
    is.na(hf_gs_verification$type_of_verification_contact)
  evidence_based_verification <- hf_gs_verification[filter, ]
  
  #Evidence based with evidence
  filter <- evidence_based_verification$study_number %in% validated_photos$study_number
  evidence_based_with_evidence <- evidence_based_verification[filter, ]
  evidence_based_with_evidence$data_team_validation <- "Validated"
  evidence_based_with_evidence$pending_validation_reason <- "NA"
  photo_validated <- nrow(evidence_based_with_evidence)
  
  #Evidence based without evidence
  gs_pending_validity <- evidence_based_verification[!filter, ]
  gs_pending_validity$data_team_validation <- "Pending Validation"
  pending_validation <- nrow(gs_pending_validity)
  
  #Supervisor  approved without evidence - no evidence required
  filter <- hf_gs_verification$type_of_verification_contact == "supervisor-approved without evidence" &
    !is.na(hf_gs_verification$type_of_verification_contact)
  supervisor_approved_no_evidence <- hf_gs_verification[filter, ]
  supervisor_approved_no_evidence$data_team_validation <- "supervisor approved without evidence"
  supervisor_approved_without_evidence <- nrow(supervisor_approved_no_evidence)
  
  gs_valid_and_invalid <- rbind(supervisor_approved_no_evidence,
                                phone_call_verification,
                                home_visit_with_evidence,
                                home_visit_without_evidence,
                                evidence_based_with_evidence,
                                gs_pending_validity
                                )
  
    to_google <- gs_valid_and_invalid
    
    range_write(verification_sheet,
                to_google,
                sheet = hf,
                range = hf,
                col_names = T,
                reformat = F
                )
    
    valid_count <- data.frame(hf,
                              supervisor_approved_without_evidence,
                              phone_verification,
                              home_verification,
                              photo_validated,
                              pending_validation
                              )
    
    hf_validation <- rbind(hf_validation, valid_count)
    
    validation_list <- rbind(validation_list, to_google)
}
    
#Summary by staff

filter <- validation_list$data_team_validation == "supervisor approved without evidence"
supervisor_approved_no_evidence_list <- validation_list[filter, ]
supervisor_approved_no_evidence_staff <- (supervisor_approved_no_evidence_list %>% count(hh_interviewer_id))
colnames(supervisor_approved_no_evidence_staff) <- c("m18_interviewer", "supervisor_approved_without_evidence")


filter <- validation_list$data_team_validation == "c/o_supervisor"
phone_list <- validation_list[filter, ]
phone_staff <- (phone_list %>% count(hh_interviewer_id))
colnames(phone_staff) <- c("m18_interviewer", "phone_verification")

filter <- validation_list$type_of_verification_contact == "home visit" &
  !is.na(validation_list$type_of_verification_contact)
home_list <- validation_list[filter, ]
home_staff <- (home_list %>% count(hh_interviewer_id))
colnames(home_staff) <- c("m18_interviewer", "home_verification")

filter <- validation_list$data_team_validation == "Validated"
valid_list <- validation_list[filter, ]
valid_staff <- (valid_list %>% count(hh_interviewer_id))
colnames(valid_staff) <- c("m18_interviewer", "validated")

filter <- validation_list$data_team_validation == "Pending Validation"
invalid_list <- validation_list[filter, ]
invalid_staff <- (invalid_list %>% count(hh_interviewer_id))
colnames(invalid_staff) <- c("m18_interviewer", "pending_validation")


staff <- merge(supervisor_approved_no_evidence_staff,
               phone_staff,
               by = "m18_interviewer",
               all=TRUE)

staff <- merge(staff,
                   home_staff,
                   by = "m18_interviewer",
                   all=TRUE)

staff <- merge(staff,
               valid_staff,
               by = "m18_interviewer",
               all=TRUE
               )
staff <- merge(staff,                  
               invalid_staff,
               by = "m18_interviewer",
               all=TRUE
               )
staff[is.na(staff)] <-  0

staff$validation_ratio <- round(((staff$supervisor_approved_without_evidence +
                                   staff$phone_verification +
                                   staff$home_verification +
                                   staff$validated) /
                                   (staff$supervisor_approved_without_evidence +
                                     staff$phone_verification +
                                      staff$home_verification +
                                      staff$validated +
                                      staff$pending_validation
                                    ))* 100, 2)
staff <- staff[order(staff$validation_ratio), ]
staff$validation_ratio <- paste(staff$validation_ratio, "%")

range_write(verification_sheet,
            staff,
            sheet = 'summary_by_staff',
            range = 'summary_by_staff',
            col_names = T,
            reformat = F
            )

#Summary by HF

hf_validation$validation_ratio <- round(((hf_validation$supervisor_approved_without_evidence +
                                            hf_validation$phone_verification +
                                                  hf_validation$home_verification+
                                                  hf_validation$photo_validated) /
                                                 (hf_validation$supervisor_approved_without_evidence + 
                                                    hf_validation$phone_verification +
                                                    hf_validation$home_verification+
                                                    hf_validation$photo_validated +
                                                    hf_validation$pending_validation)) * 100, 2)

hf_validation$validation_ratio <- paste( hf_validation$validation_ratio, "%")
range_write(verification_sheet,
            hf_validation,
            sheet = 'summary_by_hf',
            range = 'summary_by_hf',
            col_names = T,
            reformat = F
            )

# Addition for participants not seen  -------------------------------------

#Absent participants
#filter <- children_not_seen$hh_why_not_child_seen == 1
#absent <- children_not_seen[filter, c('pk', 'hf', 'hh_date', 'hh_interviewer_id')]
#absent$status <- "absent"

#Migrated 
#filter <- children_not_seen$hh_why_not_child_seen == 4
#migrated <- children_not_seen[filter, c('pk', 'hf', 'hh_date',  "hh_interviewer_id")]
#migrated$status <- "migrated"

#not_seen <- rbind(absent, migrated)

#Study numbers
#filter <- !is.na(data$study_number)
#sn <- data[filter, c('pk', 'study_number')]

#not_seen <- merge(not_seen, sn)


#Check whether child was seen later
#filter <- not_seen$status %in% seen_children$status
#not_seen_but_seen_later <- not_seen[filter, ]

# @ 11-01-2022 - nrow(not_seen_but_seen_later) = 0

#check whether phone call was made later
#filter <- data$fu_type == 1 & !is.na(data$fu_type == 1)
#phone_calls <- data[filter, ]

#filter <- not_seen$pk %in% phone_calls$pk
#not_seen_phone_later <- not_seen[filter, ]

# @ 11-01-2022 - nrow(not_seen_phone_later) = 0
  
#---------
#not_seen <- select(not_seen, -"pk")


#migrated_absent_gs <- "https://docs.google.com/spreadsheets/d/1UO_hbpvi-1lYDMmlWBeiKUxLMi5zIjqQom-2RczkIxc"  
#for(hf in hfs){
#  filter <- not_seen$hf == hf
#  hf_not_seen <- not_seen[filter, c("study_number", "status", "hh_interviewer_id")]
  
#  if(nrow(hf_not_seen) > 0){
#    hf_not_seen$reachable_or_not <- ""
#    hf_not_seen$comment <- ""
#    range_write(migrated_absent_gs,
#                hf_not_seen,
#                sheet = hf,
#                range = hf,
#                col_names = T,
#                reformat = F
#  )
#    }
#  
#}

# -------------------------------------------------------------------------
