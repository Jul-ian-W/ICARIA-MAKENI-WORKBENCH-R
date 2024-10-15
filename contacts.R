library(redcapAPI)
library(dplyr)     # group_by and filter
library(lubridate) # now
library(tidyr)
source("../icaria_project_tokens.R")


# PART1: Define data access parameters
# Both kRedcapAPIURL and kRedcapTokens are stored in the tokens.R file

# PART2: Export and bind data from all health facilities
my.fields <- c(  'record_id',
  'study_number',
  'child_surname',
  'child_fu_status',
  'child_first_name',
  'child_other_names',
  'mother_surname',
  'mother_first_name',
  'mother_other_names',
  'father_surname',
  'father_first_name',
  'father_other_names',
  'mother_caretaker',
  'caretaker',
  'other_caretaker',
  'caretaker_surname',
  'caretaker_first_name',
  'caretaker_other_names',
  'household_head',
  'other_household_head',
  'household_head_caretaker',
  'household_head_surname',
  'household_head_first_name',
  'household_head_other_names',
  'community',
  'other_community',
  'address',
  'phone_1',
  'phone_1_contact',
  'phone_2',
  'phone_2_contact'
  )

my.epi.events <- c(
  'epipenta1_v0_recru_arm_1'
  )

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






# Get study numbers
filter <- !is.na(data$study_number)
data <- data[filter, ]



#Remove migrated
data$child_fu_status <- substr(data$child_fu_status, 1, 3)
filter <- data$child_fu_status == "Mig" & !is.na(data$child_fu_status)
migrated <- data[filter, "pk"]

filter <- !(data$pk %in% migrated)
data <- data[filter, ]


#Arranging participants' contact
contact <- data[, c("pk",
                    "hf",
                    "study_number",
                    "child_surname",
                    "child_first_name",
                    "child_other_names",
                    "mother_surname",
                    "mother_first_name",
                    "mother_other_names",
                    "father_surname",
                    "father_first_name",
                    "father_other_names",
                    "mother_caretaker",
                    "caretaker",
                    "other_caretaker",
                    "caretaker_surname",
                    "caretaker_first_name",
                    "caretaker_other_names",
                    "household_head",
                    "other_household_head",
                    "household_head_caretaker",
                    "household_head_surname",
                    "household_head_first_name",
                    "household_head_other_names",
                    "community",
                    "other_community",
                    "address",
                    "phone_1",
                    "phone_1_contact",
                    "phone_2",
                    "phone_2_contact"
                    )
                ]

#Participant's name

contact$child_first_name[which(is.na(contact$child_first_name))] <- ""
contact$child_other_names[which(is.na(contact$child_other_names))] <- ""
contact$child_surname[which(is.na(contact$child_surname))] <- ""
contact$child_name <- paste(contact$child_first_name, contact$child_other_names, contact$child_surname)

#Mother's name
contact$mother_first_name[which(is.na(contact$mother_first_name))] <- ""
contact$mother_other_names[which(is.na(contact$mother_other_names))] <- ""
contact$mother_surname[which(is.na(contact$mother_surname))] <- ""
contact$mother_name <- paste(contact$mother_first_name, contact$mother_other_names, contact$mother_surname)

#father's name
contact$father_first_name[which(is.na(contact$father_first_name))] <- ""
contact$father_other_names[which(is.na(contact$father_other_names))] <- ""
contact$father_surname[which(is.na(contact$father_surname))] <- ""
contact$father_name <- paste(contact$father_first_name, contact$father_other_names, contact$father_surname)

#caretaker's name
contact$caretaker_first_name[which(is.na(contact$caretaker_first_name))] <- ""
contact$caretaker_other_names[which(is.na(contact$caretaker_other_names))] <- ""
contact$caretaker_surname[which(is.na(contact$caretaker_surname))] <- ""
contact$caretaker_name <- paste(contact$caretaker_first_name, contact$caretaker_other_names, contact$caretaker_surname)


#household_head's name  (later not considered)
contact$household_head_first_name[which(is.na(contact$household_head_first_name))] <- ""
contact$household_head_other_names[which(is.na(contact$household_head_other_names))] <- ""
contact$household_head_surname[which(is.na(contact$household_head_surname))] <- ""
contact$household_head_name <- paste(contact$household_head_first_name, contact$household_head_other_names, contact$household_head_surname)

#contact$household_head[(contact$household_head =="1")] <- "Mother"
#contact$household_head[(contact$household_head =="2")] <- "Father"

#contact$household_head[which(contact$household_head !="88" & !is.na(contact$other_household_head))] <- "INCONCLUSIVE"
#contact$household_head[which(contact$household_head !="88" & !is.na(contact$other_household_head))] <- contact$other_household_head

#filter <- contact$household_head =="88"
#other_hh_head <- contact[filter, ]
#other_hh_head$household_head <- other_hh_head$other_household_head

#contact <- contact[!filter, ]
#contact <- rbind(contact, other_hh_head )


#Phone number
contact$phone_1[which(is.na(contact$phone_1))] <- ""
contact$phone_2[which(is.na(contact$phone_2))] <- ""
contact$phone_numbers <- paste(contact$phone_1, "__", contact$phone_2)

#Phone contact
contact$phone_1_contact[which(is.na(contact$phone_1_contact))] <- ""
contact$phone_2_contact[which(is.na(contact$phone_2_contact))] <- ""
contact$phone_contacts <- paste(contact$phone_1_contact, "__", contact$phone_2_contact)


contact$caretaker_relationship <- rowSums(contact[, c("mother_caretaker", "caretaker")], na.rm = T)

#Mothers

filter <- contact$caretaker_relationship == "1"
mother_contact <- contact[filter, ]
mother_contact$caretaker_name <- mother_contact$mother_name
mother_contact$caretaker_relationship  <- "Mother"

#Fathers

filter <- contact$caretaker_relationship == "2"
father_contact <- contact[filter, ]
father_contact$caretaker_name <- father_contact$father_name
father_contact$caretaker_relationship  <- "Father"

#Other

filter <- contact$caretaker_relationship == "88"
other_contact <- contact[filter, ]
other_contact$caretaker_relationship  <- other_contact$other_caretaker


#Error
filter <- !(contact$caretaker_relationship == "88"| contact$caretaker_relationship == "1"|contact$caretaker_relationship == "2")
error_contact <- contact[filter, ]
error_contact$caretaker_relationship  <- "INCONCLUSIVE"
error_contact$caretaker_name  <- "TO BE CONFIRMED"

#combine ever ycontact
contact <- rbind(mother_contact,father_contact,other_contact, error_contact)


contact <- contact[, c("hf",
                       "study_number",
                       "child_name",
                       "caretaker_name",
                       "caretaker_relationship",
                       "phone_numbers",
                       "phone_contacts",
                       "address"
                       )
                   ]

contacts <- contact[, c("study_number",
                        "child_name",
                        "caretaker_name",
                        "caretaker_relationship",
                        "phone_numbers",
                        "phone_contacts",
                        "address")]


write.csv(contact, "contacts.csv", row.names = F)



#my.list <- read.csv("Participants_contacted_since_02-05-2022.csv")
#my.list.numbers <- my.list[, c("hf","study_number")]
#contact_list <- merge(my.list.numbers, contact)
#contact_list <- contact_list[, c("hf",
#                                 "study_number",
#                                 "child_name",
#                                 "caretaker_relationship",
#                                 "caretaker_name",
#                                 "phone_numbers",
#                                 "phone_contacts",
#                                 "address"
#                                 )
#                             ]


#redcap_data <- read.csv("Participants_with_last_contact_before_02-05-2022.csv")
#surv_data <- read.csv("googlesheet_data.csv")

#filter <- !(redcap_data$study_number %in% surv_data$study_number)
#pending_surv <- redcap_data[filter, c("hf", "study_number")]
#pending_surv <- merge(pending_surv, contact)
#pending_surv <- pending_surv[, c("hf",
#                                  "study_number",
#                                  "child_name",
#                                  "caretaker_relationship",
#                                  "caretaker_name",
#                                  "phone_numbers",
#                                  "phone_contacts",
#                                  "address"
#                                 )
#                             ]
#

#surv_april <- read.csv("surv_11-04-2022_forward.csv")
#migrations <- read.csv("migration.csv")

#filter <- !(migrations$study_number %in% surv_april$study_number)
#pending_migrations <- migrations[filter, ]
#write.csv(pending_migrations, "Pending Migrations.csv", row.names = F)

#migrations <- read.csv("migration.csv")

#lungi <- read.csv("ICARIA Migration Destination HF01.csv")
#lunsar <- read.csv("ICARIA Migration Destination HF02.csv")
#masiaka <- read.csv("ICARIA Migration Destination HF04.csv")
#port_loko <- read.csv("ICARIA Migration Destination HF05.csv")
#magburaka <- read.csv("ICARIA Migration Destination HF08.csv")
#loreto <- read.csv("ICARIA Migration Destination HF11.csv")
#red_cross <- read.csv("ICARIA Migration Destination HF12.csv")
#stocco <- read.csv("ICARIA Migration Destination HF13.csv")
#mgh <- read.csv("ICARIA Migration Destination HF16.csv")

#lungi <- merge(lungi, contact)
#lunsar <- merge(lunsar, contact)
#masiaka <- merge(masiaka, contact)
#port_loko <- merge(port_loko, contact)
#magburaka <- merge(magburaka, contact)
#loreto <- merge(loreto, contact)
#red_cross <- merge(red_cross, contact)
#stocco <- merge(stocco, contact)
#mgh <- merge(mgh, contact)
