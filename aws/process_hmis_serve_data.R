#!/usr/bin/env Rscript

# Load necessary libraries
library(HMISserve)
library(HMISprep)
library(HMISdata)
library(logger)
library(dplyr)
library(stringr)

# Set up logging
logger::log_info("Starting HMIS serving job")

# Error handling wrapper
tryCatch({
  log_info("Serving HMIS data:")

  log_info("Downloading Data:")
  
  Project = HMISdata::load_hmis_parquet("Project.parquet")
  Client = HMISdata::load_hmis_parquet("Client.parquet")
  Inventory = HMISdata::load_hmis_parquet("Inventory.parquet")
  Enrollment_extra_Client_Exit_HH_CL_AaE = HMISdata::load_hmis_parquet("Enrollment_extra_Client_Exit_HH_CL_AaE.parquet")
  Disabilities = HMISdata::load_hmis_parquet("Disabilities.parquet")
  Referrals = HMISdata::load_hmis_parquet("Referrals.parquet")
  Services_enroll_extras = HMISdata::load_hmis_parquet("Services_enroll_extras.parquet")
  co_clients_served = HMISdata::load_hmis_parquet("co_clients_served.parquet")
  IncomeBenefits = HMISdata::load_hmis_parquet("IncomeBenefits.parquet")
  HealthAndDV = HMISdata::load_hmis_parquet("HealthAndDV.parquet")
  VeteranCE = HMISdata::load_looker_data(filename = "Client", col_types = HMISdata::look_specs$Client)
  tay = HMISdata::load_hmis_parquet("tay.parquet")
  Contacts = HMISdata::load_hmis_parquet("Contacts.parquet")
  Funder = HMISdata::load_hmis_parquet("Funder.parquet")
  Referrals_full = HMISdata::load_hmis_parquet("Referrals_full.parquet")
  Scores = HMISdata::load_hmis_parquet("Scores.parquet")
  Users = HMISdata::load_hmis_parquet("Users.parquet")
  living_situation = HMISprep::living_situations
  destinations = HMISprep::destinations
  mahoning_projects = HMISdata::load_hmis_parquet("mahoning_projects.parquet")
  rm_dates = HMISprep::load_dates()
  guidance = HMISserve::get_guidance()
  data_types = HMISprep::data_types
  vars = HMISserve::make_vars()
  check_fns = HMISserve::relevant_dq()
  Regions = HMISdata::Regions

  guidance_tibble <- tibble::enframe(guidance) # convert to tibble

  HMISdata::upload_hmis_data(guidance_tibble, "guidance.parquet", format = "parquet",
bucket = "shiny-data-cohhio", folder = "RME")

  logger::log_info("Running bed utilization...")
  # Bed Utilization ####
  HMISserve::bed_unit_utilization(Project = Project, 
    Inventory = Inventory, 
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE, 
    rm_dates = rm_dates, 
    data_types = data_types)

  logger::log_info("Running client counts...")
  # Client Counts ####
  HMISserve::client_counts(Project = Project, 
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
    Disabilities = Disabilities,
    Referrals = Referrals,
    rm_dates = rm_dates)

  logger::log_info("Running QPR...")
  # QPR ####
  HMISserve::qpr_ees(Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
    Project = Project,
    Services_enroll_extras = Services_enroll_extras,
    destinations = destinations,
    IncomeBenefits = IncomeBenefits,
    rm_dates = rm_dates)
  
  HMISserve::qpr_spdats(Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
    Project = Project,
    Regions = Regions,
    Scores = Scores,
    data_types = data_types)
  
  logger::log_info("Running prioritization...")
  # Prioritization ####
  HMISserve::prioritization(co_clients_served = co_clients_served,
    Disabilities = Disabilities,
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
    HealthAndDV = HealthAndDV,
    IncomeBenefits = IncomeBenefits,
    Project = Project,
    Referrals = Referrals,
    Scores = Scores,
    data_types = data_types)

  logger::log_info("Running veterans report...")
  # Veterans ####
  HMISserve::vets(Client = Client,
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
    Project = Project,
    VeteranCE = VeteranCE,
    tay = tay)

  # Data Quality ####
  deps <- list(Client = Client,
    Project = Project,
    Contacts = Contacts,
    Disabilities = Disabilities, 
    Inventory = Inventory, 
    HealthAndDV = HealthAndDV,
    IncomeBenefits = IncomeBenefits,
    Funder = Funder,
    Referrals = Referrals,
    Referrals_full = Referrals_full,
    Scores = Scores,
    Users = Users,
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
    Services_enroll_extras = Services_enroll_extras,
    living_situation = living_situation,
    guidance = guidance,
    mahoning_projects = mahoning_projects,
    vars = vars,
    rm_dates = rm_dates,
    check_fns = check_fns)
  
  logger::log_info("Running data quality...")

  HMISserve::data_quality_summary(co_clients_served = co_clients_served, rm_dates = rm_dates, .deps = deps)

  logger::log_info("Running veteran active list...")
  # Veteran Active ####
  HMISserve::vet_active(co_clients_served = co_clients_served,
    Project = Project,
    VeteranCE = VeteranCE,
    Contacts = HMISdata::load_hmis_parquet("Contacts.parquet"),
    Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE)

  # Project Evaluation ####

  # logger::log_info("Running Mahoning competition repot...")
  # HMISserve::project_evaluation_mahoning(Project = Project,
  #   Funder = Funder,
  #   Enrollment_extra_Client_Exit_HH_CL_AaE = Enrollment_extra_Client_Exit_HH_CL_AaE,
  #   rm_dates = rm_dates)
  # logger::log_info("Mahoning competition report completed")
  
  # SPMs

  logger::log_info("HMIS data processing completed successfully")
}, error = function(e) {
  logger::log_error("Error in HMIS data processing: {e$message}")
  # Optional: send notification about failure
  quit(status = 1)
})
