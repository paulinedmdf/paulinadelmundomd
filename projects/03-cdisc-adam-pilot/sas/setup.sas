/******************************************************************************
 * Program  : setup.sas
 * Purpose  : Define libnames and the project format catalog for CDISC Pilot 01.
 * Inputs   : ../data/sdtm/, ../data/adam_r/ (R reference output)
 * Outputs  : LIBNAME SDTM, LIBNAME ADAM, LIBNAME RREF, FMTLIB.PILOT
 * ADaM IG  : —
 *****************************************************************************/

%let proj = /home/&sysuserid/cdisc-pilot;   /* SAS OnDemand layout */

libname sdtm xport "&proj/data/sdtm.xpt"  access=readonly;
libname adam      "&proj/adam";
libname rref xport "&proj/data/adam_r.xpt" access=readonly;

proc format library=adam.fmtlib;
  value $sex   "M" = "Male" "F" = "Female";
  value $arm   "Placebo"          = "Placebo"
               "Xanomeline Low Dose"  = "Donepezil 5 mg"
               "Xanomeline High Dose" = "Donepezil 10 mg";
  value agegrn 1 = "<65" 2 = "65-80" 3 = ">80";
run;

options fmtsearch=(adam.fmtlib);
