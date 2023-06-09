setwd("C:/Users/Julian Williams/Desktop/Makeni_training/R/ICARIA-MAKENI/REDCap identifier")


library(redcapAPI)
library(dplyr)     # group_by and filter
library(lubridate) # now
library(tidyr)
library(googlesheets4)
source("../icaria_records_identifier_tokens.R")
source("records_identifier_referrals.R")

gs4_auth()
1

#The record identifier sheet on google drive is associated with:
  #identifier_sheet    variable

my.fields <- c('record_id',
               'study_number',
               'child_fu_status'
               )

my.epi.events <- c(
  'epipenta1_v0_recru_arm_1'
  )

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
identifiers <- c("record_id", "study_number")

filter <- !is.na(hf.data$study_number)
#Sort by study number
participants_identification <- hf.data[filter, my.fields]
 print("writing to gs")
range_write(identifier_sheet,
            participants_identification,
            sheet = hf,
            range = hf,
            col_names = T,
            reformat = F
)
}

