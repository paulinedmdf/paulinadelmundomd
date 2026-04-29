/******************************************************************************
 * Program  : xpt_export.sas
 * Purpose  : Export ADaM datasets to SAS V5 transport (XPT) for the FDA.
 *            These are the files read by the Quarto reconciliation chunk.
 * Inputs   : ADAM.ADSL, ADAM.ADAE, ADAM.ADLBC
 * Outputs  : output/adsl.xpt, output/adae.xpt, output/adlbc.xpt
 *****************************************************************************/

%include "setup.sas";

%macro export_xpt(ds=);
  libname _x xport "output/&ds..xpt";
  data _x.&ds; set adam.&ds; run;
  libname _x clear;
%mend;

%export_xpt(ds=adsl);
%export_xpt(ds=adae);
%export_xpt(ds=adlbc);
