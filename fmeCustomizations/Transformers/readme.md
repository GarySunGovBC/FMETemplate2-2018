# BCDC file change detection

## Introduction

BCDC file change detection re-uses a lot of the code that was created 
for the generic file change detection routine.  The logic for the file
change detection log stays the same.  

The only real difference between the generic file change detection and 
the BCDC file change detection is how the change dates are retrieved.

Generic File change detection gets the date from the filesystems metadata.

BCDC file change detection gets the change date via a rest query to 
BCDC system.

## How it works

1.  A custom linked transformer called 'BCDC File Change Detector' will get
    inserted into an FMW
    
2.  That transformer has a python caller that is bound to the module / class
    BCDCFileChangeDetector.ChangeFlagFetcher
    
3.  BCDCFileChangeDetector.ChangeFlagFetcher for the most part re-uses the 
    same logic as File_Change_DetectorV2.ChangeFlagFetcher, except it 
    
    
