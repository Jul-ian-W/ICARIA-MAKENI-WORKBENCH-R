
# ICARIA Health Facilities ------------------------------------------------
hfs <- c("HF01","HF02","HF03","HF04","HF05","HF06","HF08","HF10","HF11","HF12", "HF13", "HF15", "HF16", "HF17")
cohort_hfs <- c("HF01","HF02","HF03","HF04","HF05","HF06","HF08","HF10","HF11", "HF13","HF16")

icaria_hf_names <- list(
  HF01 = "Lungi U5",
  HF02 = "Lunsar",
  HF03 = "Mahera",
  HF04 = "Masiaka",
  HF05 = "Port loko U5",
  HF06 = "Rogbere",
  HF08 = "Magburaka U5",
  HF10 = "Matotoka",
  HF11 = "Loreto",
  HF12 = "Red Cross",
  HF13 = "Stocco",
  HF15 = "Makama",
  HF16 = "Makeni Regional",
  HF17 = "Masuba"
)

ica_letters <- c("A", "B", "C", "D", "E", "F")



# Function to generate study numbers --------------------------------------

study_numbers <- function(icaria_data, identifiers, study_number){
  #identify a set of fields (identifiers - eg, hf, record_id, pk) and put in a vector
  filter <- !is.na(icaria_data[[study_number]])
  sn <- icaria_data[filter, c(identifiers, study_number)]
  return(sn)
}

# Combine Study number and data frame -------------------------------------

data_with_sn <- function(icaria_data, sn_data, identification_field){
  identified_data <- merge(icaria_data, sn_data)
  #identified_data[1] <- identified_data[[identification_field]]
  return(identified_data)
}


# Isolating records of a particular event ---------------------------------
  #For this all the relevant variables have to be identified and put in a vector

event_data <- function(icaria_data, event_name, event_identifier, event_fields){
  filter <- icaria_data$redcap_event_name == event_name & !is.na(icaria_data[event_identifier])
  event_data <- icaria_data[filter, event_fields]
}


alarm_fxn <- function(data_source_data, alarm_string){
  #requires a dataframe called participants_contacts with obvious variables (identifier and contacts)
  alarm_list <- filter(data_source_data, str_detect(child_fu_status, alarm_string))
  alarm_list <- alarm_list[, c("hf", "study_number")]
  alarm_list <- merge(alarm_list, participants_contacts)
}


# DATA IN FIELDS SUPPOSED TO BE HIDDEN -----------------------------

data_in_hidden_fields <- function(icaria_data, reference_field, reference_field_list, target_field){
  wrong_data_present<- data.frame()
  for(value in reference_field_list){
    filter <- (is.na(icaria_data[reference_field]) | icaria_data[reference_field] == value) & !is.na(icaria_data[target_field])
    target_frame <- icaria_data[filter, ]
    wrong_data_present <- rbind(wrong_data_present, target_frame)
  }
  return(wrong_data_present)
}


# MISSING DATA THAT IS SUPPOSED TO BE PRESENT FOR A RANGE OF VALUES-----------------------------

missing_data <- function(icaria_data, reference_field, reference_field_list, target_field){
  missing_data <- data.frame()
  for(value in reference_field_list){
    filter <- !is.na(icaria_data[reference_field]) & icaria_data[reference_field] == value & is.na(icaria_data[target_field])
    target_frame <- icaria_data[filter, ]
    missing_data <- rbind(missing_data, target_frame)
  }
  return(missing_data)
}

#MISSING DATA FOR ANY VALUE IN THE REFERENCE FIELD
missing_data_for_any_value <- function(icaria_data, reference_field, target_field){
    filter <- !is.na(icaria_data[reference_field]) & is.na(icaria_data[target_field])
    missing_data <- icaria_data[filter, ]
   return(missing_data)
}  


ica_hf_names_fxn <- function(data){
  #Create a column in data set called "hf_names"
  for(hf_name in names(icaria_hf_names)){
    data[data['hf'] == hf_name, "hf_names"] <- icaria_hf_names[[hf_name]]
  }
  return(data)
}


code_to_value <- function(data, data_variable, code_list, starting_value){
  #The "starting_value" is the first code defined in the codebook
  #count <- 0
  for(code_value in code_list){
    #count <- count + 1
    data[(data[data_variable] == starting_value) & !is.na(data[data_variable]), (data_variable)] <- code_value
    starting_value <- starting_value + 1
  }
  return(data)
}

#Use the following code when the referenced column in the code_to_value function
#has NAs

#data_combined <- data.frame()
#flag <- 0
#for(hfs_col in hfs_cols){
#  flag <- flag +1
#  separate_data <- suv.data[!is.na(suv.data[[hfs_col]]), ]
#  separate_data <- code_to_value(separate_data, hfs_col, districts[[flag]], 1)
#  separate_data$hf_name <- separate_data[[hfs_col]]
#  data_combined <- rbind(data_combined, separate_data)
#}

#FUNCTIONS FOR DISCREPANCY and WRONG ADMINISTRATIONS

compareVariables <- function(v1,v2) {
  same <- (v1 == v2) | (is.na(v1) & is.na(v2))
  same[is.na(same)] <- FALSE
  return(same)
}

intervention_discrepancy <- function(data, redcap_var, log_var, front_columns, back_columns, instruction_column, excel_file_name, sheet_name, reference_values = NULL){
  #redcap_var and log_var are corresponding variables in redcap and the log
  #for eg: int_date in redcap and Administration date on the log
  #error_string is the string displayed as succinct statement of the problem
  #reference_values are what the actual administration should be. ie the correct volume based on the weight or correct letter based on study number
  #The production of an excel file by this function requores the openxlsx package
  if(is.null(reference_values)){
    #filter_discrep <- !(data[redcap_var] == data[log_var])
    filter_discrep <- compareVariables(data[redcap_var], data[log_var])
    discrep <- data[!filter_discrep, c(front_columns,
                                       redcap_var,
                                       log_var,
                                       back_columns
    )
    ]
  } else if(!is.null(reference_values)){
    filter_discrep <- !compareVariables(data[reference_values], data[log_var]) |
      !compareVariables(data[reference_values], data[redcap_var])
    discrep <- data[filter_discrep, c(front_columns,
                                      reference_values,
                                      redcap_var,
                                      log_var,
                                      back_columns,
                                      instruction_column
    )
    ]
    if(nrow(discrep) > 0){
      filter1 <- discrep[reference_values] == discrep[redcap_var] &
        discrep[reference_values] != discrep[log_var]
      
      filter2 <- discrep[reference_values] != discrep[redcap_var] &
        discrep[reference_values] == discrep[log_var]
      
      filter3 <- discrep[reference_values] != discrep[redcap_var] &
        discrep[reference_values] != discrep[log_var] &
        discrep[redcap_var] == discrep[log_var]
      
      filter4 <- discrep[reference_values] != discrep[redcap_var] &
        discrep[reference_values] != discrep[log_var] &
        discrep[redcap_var] != discrep[log_var]
      #discrep[(discrep[reference_values] == discrep[redcap_var]), discrep[advice]] <-
      #  "correct REDCap"
      
      filter1_statement <- "correct log and google sheet"
      filter2_statement <- "if google entry is correct, correct redcap"
      filter3_statement <- "if google entry is correct, this is a deviation. Do nothing. Escalate"
      filter4_statement <- "if google entry is correct, this error is multi-dimensional. Escalate"
      
      discrep[instruction_column][filter1] <- filter1_statement
      discrep[instruction_column][filter2] <- filter2_statement
      discrep[instruction_column][filter3] <- filter3_statement
      discrep[instruction_column][filter4] <- filter4_statement
    }
    
  }
  if (nrow(discrep) > 0){
    addWorksheet(excel_file_name, sheet_name)
    writeData(excel_file_name, sheet_name, discrep, borders = "all", borderColour = "#00bfff")
    
  }
}  


date_time_to_date <- function(icaria_data, date_time_variable){
  icaria_data[date_time_variable] <- format(icaria_data[date_time_variable],
                          '%d-%m-%Y')
  return(icaria_data)
}


latest_event_data <- function(icaria_data, event_name, event_fields, instrument_identifier_date_field, primary_key, period_init, period_final){ 
  #event_fields = a list of fields for the event including the "primary key", instrument date etc
  filter <- icaria_data$redcap_event_name == event_name &
    !is.na(icaria_data[instrument_identifier_date_field])&
    icaria_data[[instrument_identifier_date_field]] >= period_init &
    icaria_data[[instrument_identifier_date_field]] <= period_final
  
  
  visit_data <- icaria_data[filter, c(event_fields) ]
  
  visit_data <- visit_data[order(visit_data[[instrument_identifier_date_field]], decreasing = TRUE), ]
  latest_visit_data <- visit_data[!duplicated(visit_data[[primary_key]]), ]
  
  #Learn to use dplyr functions within your functions
  
}

count_events <- function(data, event, value){
  events <- data[data[event] == value & !is.na(data[event]), ]
  count <- nrow(events)
  return(count)
}


# checking Multiply antigen sequence
#1. multiple dose antigens
#2. Antigens administered during the same visit

multi_dose_antigen_receipt_sequence <- function(antigen_data, identifier, antigen_list, received = 1, not_received = 0){
  #Define the variables for the various administrations of the antigen in ascending order
  #The identifier is like the record id, to identify the particioant
  antigen_errors <- data.frame()#(c(identifier, antigen_list))
  antigen_index <- 0
  last_sp <- NULL
  for(antigen in antigen_list){
    
    if(antigen_index > 0){
      error <- filter(antigen_data, ((antigen_data[[last_sp]] == not_received | is.na(antigen_data[[last_sp]])) & antigen_data[[antigen]] == received))
      error <- error[, c(identifier,
                         last_sp,
                         antigen)]
      if(nrow(error) > 0){
        antigen_errors <- bind_rows(antigen_errors, error)
        antigen_errors <- antigen_errors[!is.na(antigen_errors[identifier]), ]
      }
      
    }
    last_sp <- antigen
    antigen_index <- antigen_index + 1
  }
  return(antigen_errors)
}


contemporary_antigens <- function(antigen_data, identifier, antigen_list, received = 1, not_received = 0){
  #Only useful for visits with more than 1 vaccine administration
  #This function considers that the positive responses are "1" and the negative responses are "0"
  #It also works for only 2 possible responses ("1" and "0")
  #antigen_list :Antigens administered on the same day
  #The identifier is like the record id, to identify the particioant
  same_day_antigens <- antigen_data[, c(identifier,
                                   antigen_list)
                               ]
  
  filter_errors <- !(rowSums(same_day_antigens[, antigen_list]) == length(antigen_list) |
    rowSums(same_day_antigens[, antigen_list]) == 0)
  
  same_day_antigens_error <- same_day_antigens[filter_errors, ]
  same_day_antigens_error <- same_day_antigens_error[!is.na(same_day_antigens_error[identifier]), ]
  
  #same_day_Antigen_discrep <- same_day_Antigen[length(unique(same_day_Antigen[[antigen_list]])) != 1, ]
  #return(same_day_antigens)
  return(same_day_antigens_error)
}
#produce u5 cards
#earlier antigens not received but later ones administered

