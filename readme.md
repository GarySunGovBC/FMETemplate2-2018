# DataBC FME Framework

This is the root directory for the DataBC FME Framework.  The 
Goals of the template are:

1. Promote Modularity:  Keep code embedded in FMWs to an extreme minimum.  All 
    logic, etc is stored in external modules.  This approach allows us to very 
    easily add new features or bug fixes to the framework without having to edit
    a single FMW.
2. Make FME source / destination features transparent.  Strict parameter standard
    allows easy retrieval of sources and destinations without ever having to open
    the actual FMW.
3. Update to use the most current FME python libraries. 

   
# About

## FME Template - User Documentation
This is a guide to allow FME developers to get started using our new FME 
template.  Be sure you have at least scanned
this document before continuing on to read about other pieces related
to the template .

https://docs.google.com/document/d/1WXUMN-2RDC_7u3OgtbG-NYFevKqZcZlxwF4PEB_lpu4/edit?usp=sharing

## FME Template - Technical Documentation (somewhat outdated)
This is a more indepth description of how various pieces that make up the
template function.  Where config files are located, how they are used, how 
change detection works. etc...

This is a much less polished document as is not intended as a publicly 
consumable document.  Its been a scratch space to put documentation 
in lieu of having another place to put it which the template was under
development.  Eventually the goal is to move everything in 
this document into appendices in the User Documentation.

(https://docs.google.com/document/d/1U31K0jl9dTwqbNZJuajbLf-dhmNlrCzpXZBrfxKlgJY/edit?usp=sharing)

Much of this documentation is getting merged with the user docs
referenced above

## [FME Development and Deployment](https://wiki.pathfinder.gov.bc.ca/delta-ops/ETL/FME/fme-template-administration)


# Description
What follows is a description of the various directories and scripts 
that make up the template.

## ./

### DataBCFMWTemplate.py
This is the module that contains all the code necessary to implement the
various python functions used by FMW's that implement the template.

### FMELogger.py
Contains a wrapper that allows regular fme log messages to be forwarded
to the fme log file.

### usercustomize.py
Development of the framework is completed using FME Workbench.  This module sets
up paths used by the module.  The primary purpose of this module is to enable 
development to take place on the same machine without effecting other peoples
scripts on that machine.

### ChangeDetectLib.py
The module with the code necessary for the custom file change detection 
to work.  Only contains the methods, the actual change detect code is with 
the "Change Detect" custom transformer in the fmeCustomizations\Transformers
folder.

## ./config

Where the various config files related to the template are stored

### dbCreds.json
This is an optional file that is used when FMW scripts are in in development.
When scripts are run on a machine other than arneb or matar the password
credentials will be retrieved from this file.

### templateDefaults.config
This file is well documented by comments.  A small subset of aspects that the 
file controls include:
  - relative file paths to other aspects of the template
  - acceptable keywords to be used to indicate whether the script should
    be run against delivery, test or production environments.
  - database connection parameters to be used when running in dlv, test, or
    production modes.
  - pmp tokens used to gain access to passwords
  
### templateLogging.config
This is a python logging config file amarks described [here](https://docs.python.org/2/library/logging.config.html):
If anything needs to be changed relating to how logging is configured
for the template it should be done in this file.  Example:
  - changing log levels
  - creating a new output log (console, file, etc...)
  - change log message format
  - and much much more!
  
## ./fmeCustomizations
Currently the only customizations that the template uses is "custom linked transformers".
FME Workbench requires you create all the directories in contained 
in this folder in order to use FME cutomizations.  When developing 
outside of DataBC firewalls (example on a standard GTS) you may have 
to configure FME to use these custom folders.

To add this path to FME: Tools->FME Options->Default Paths, then add the 
folder to the "Shared FME Folders".

A process has been created that automatically deploys these folders
to FME server.

## ./lib_ext
This is where external dependencies should be put.  The requirements.txt
file that is found in this directory contains dependencies for this the 
actual template code as well as the dependencies of DataBC Libs used by 
the template.

## ./outputs
This is where change detection logs will be stored as well as any failed
features files.  The template will generate this directory automatically 
then the fme script only needs to consume it.

## ./tests
Unit tests, and other code used to evaluate functionality of the 
template.

## ./util
utility scripts used for implementation of the template.  These are described
below.  

### ServerSync.py
The script used to sync the various components used by the template 
with FME Server. 






 
