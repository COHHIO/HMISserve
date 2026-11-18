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
  
  Project = HMISserve::hs_read("Project.parquet")
  Client = HMISserve::hs_read("Client.parquet")
  Inventory = HMISserve::hs_read("Inventory.parquet")
  assessments = HMISserve::hs_read("Assessment.csv", kind = "hmis_csv")
  Enrollment_extra_Client_Exit_HH_CL_AaE = HMISserve::hs_read("Enrollment_extra_Client_Exit_HH_CL_AaE.parquet")
  Disabilities = HMISserve::hs_read("Disabilities.parquet")
  Referrals = HMISserve::hs_read("Referrals.parquet")
  Services_enroll_extras = HMISserve::hs_read("Services_enroll_extras.parquet")
  co_clients_served = HMISserve::hs_read("co_clients_served.parquet")
  IncomeBenefits = HMISserve::hs_read("IncomeBenefits.parquet")
  HealthAndDV = HMISserve::hs_read("HealthAndDV.parquet")
  VeteranCE = HMISserve::hs_read("Client", kind = "looker", col_types = HMISdata::look_specs$Client)
  tay = HMISserve::hs_read("tay.parquet")
  Contacts = HMISserve::hs_read("Contacts.parquet")
  Funder = HMISserve::hs_read("Funder.parquet")
  Referrals_full = HMISserve::hs_read("Referrals_full.parquet")
  Scores = HMISserve::hs_read("Scores.parquet")
  Scores_enrollments = HMISserve::hs_read("Scores_enrollments.parquet")
  Users = HMISserve::hs_read("Users.parquet")
  living_situation = HMISprep::living_situations
  destinations = HMISprep::destinations
  mahoning_projects = HMISserve::hs_read("mahoning_projects.parquet")
  rm_dates = HMISprep::load_dates()
  guidance = HMISserve::get_guidance()
  data_types = HMISprep::data_types
  vars = HMISserve::make_vars()
  check_fns = HMISserve::relevant_dq()
  Regions = HMISdata::Regions

  guidance_tibble <- tibble::enframe(guidance) # convert to tibble

  HMISdata::upload_hmis_data(guidance_tibble, "guidance.parquet", format = "parquet",
bucket = "shiny-data-cohhio", folder = Sys.getenv("DATA_ENV", unset = "RME"))

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
    assessments = assessments,
    Disabilities = Disabilities, 
    Inventory = Inventory, 
    HealthAndDV = HealthAndDV,
    IncomeBenefits = IncomeBenefits,
    Funder = Funder,
    Referrals = Referrals,
    Referrals_full = Referrals_full,
    Scores = Scores,
    Scores_enrollments = Scores_enrollments,
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
    Contacts = Contacts,
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
  logger::log_error("Error in HMIS data processing: {rlang::cnd_message(e)}")
  # Optional: send notification about failure
  quit(status = 1)
})