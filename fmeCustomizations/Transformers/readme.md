# DataBC Custom Transfomers

This directory contains a list of all the DataBC custom transformers.
Many of these transformers use a "Python caller" in them.  The best
practice for dealing with these types of transformers is to keep as
much of the code external to the class that the python caller implements.

The python code gets stored in moduels with the name *Lib in them in 
the main template directory.

Each of these transformers may have dependencies that require them to 
be implemented in an environment that implements the DataBC FMW template.

See this doc for more on how to implement that.

https://docs.google.com/document/d/1WXUMN-2RDC_7u3OgtbG-NYFevKqZcZlxwF4PEB_lpu4/edit?usp=sharing

# Transformers In This Directory

## Parcel Map Updater

### Introduction / Description

This transformer will get the path to the current staging area for 
the parcel map data.  The script then:

  - Requests a provincial data set for the parcel map data
  - montors that request for completion
  - once complete initiates a download
  - Compares the downloaded data with the previously downloaded data
    if it has changed then the data is updated in the staging area.
    
### How it Works:

The transformer will 

## File Change Detector

### Introduction / Description

This transformer will create a log file in the directory specified by the 
template which once in production will be:

\\databc.bcgov\work\scripts\python\DataBCFmeTemplate2\outputs\changelogs

The idea is that this transformer will detect if a file has changed and
if it has then it gets updated if it has not then the script will end 
at the change transformer level.

### How it works

Looks at the file change date of the source file.  Compares it against a 
log file of file change dates.  If the change date on the file is newer
than the one in the change log then the file will get updated into the 
BCGW.

##BCDC file change detection

### Introduction / Description

BCDC file change detection re-uses a lot of the code that was created 
for the generic file change detection routine.  The logic for the file
change detection log stays the same.  

The only real difference between the generic file change detection and 
the BCDC file change detection is how the change dates are retrieved.

Generic File change detection gets the date from the filesystems metadata.

BCDC file change detection gets the change date via a rest query to 
BCDC system.

### How it works

1.  A custom linked transformer called 'BCDC File Change Detector' will get
    inserted into an FMW
    
2.  That transformer has a python caller that is bound to the module / class
    BCDCFileChangeDetector.ChangeFlagFetcher
    
3.  BCDCFileChangeDetector.ChangeFlagFetcher for the most part re-uses the 
    same logic as File_Change_DetectorV2.ChangeFlagFetcher, except it 
    

