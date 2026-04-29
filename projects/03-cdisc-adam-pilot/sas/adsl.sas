/******************************************************************************
 * Program  : adsl.sas
 * Purpose  : Derive ADSL (Subject-Level Analysis Dataset) per ADaM IG v1.3 §3.1
 *            and the CDISC Pilot 01 ADSL specification.
 * Inputs   : SDTM.DM, SDTM.EX, SDTM.SV, SDTM.SUPPDM
 * Output   : ADAM.ADSL
 * Notes    : One record per USUBJID. TRT01P / TRT01A from DM.ARM / DM.ACTARM.
 *            Treatment dates come from EX (first non-zero dose / last non-zero
 *            dose). Population flags follow the Pilot SAP §6.
 *****************************************************************************/

%include "setup.sas";

/*----- Treatment epoch from EX -------------------------------------------*/
proc sql;
  create table _trtdt as
    select STUDYID, USUBJID,
           min(input(EXSTDTC, e8601da.)) as TRTSDT format=date9.,
           max(input(EXENDTC, e8601da.)) as TRTEDT format=date9.
    from sdtm.ex
    where EXDOSE > 0
    group by STUDYID, USUBJID;
quit;

/*----- Build ADSL --------------------------------------------------------*/
data adam.adsl (label="Subject-Level Analysis Dataset");
  merge sdtm.dm (in=in_dm)
        _trtdt;
  by STUDYID USUBJID;
  if in_dm;

  /* Treatment variables — IG v1.3 §3.1.1 */
  length TRT01P TRT01A $40;
  TRT01P = ARM;
  TRT01A = ACTARM;
  TRT01PN = case
              when ARM = "Placebo" then 0
              when ARM = "Xanomeline Low Dose"  then 54
              when ARM = "Xanomeline High Dose" then 81
              else .
            end;
  TRT01AN = TRT01PN;

  /* Treatment duration in days (inclusive) */
  if not missing(TRTSDT) and not missing(TRTEDT) then
    TRTDURD = TRTEDT - TRTSDT + 1;

  /* Population flags — IG v1.3 §3.1.4 */
  length SAFFL ITTFL $1;
  SAFFL = ifc(not missing(TRTSDT), "Y", "N");
  ITTFL = ifc(not missing(ARMCD) and ARMCD ne "Scrnfail", "Y", "N");

  /* Age groupings per Pilot SAP */
  length AGEGR1 $5;
  if      AGE <  65 then do; AGEGR1 = "<65";   AGEGR1N = 1; end;
  else if AGE <= 80 then do; AGEGR1 = "65-80"; AGEGR1N = 2; end;
  else if not missing(AGE) then do; AGEGR1 = ">80";  AGEGR1N = 3; end;

  format TRTSDT TRTEDT date9.;
  keep STUDYID USUBJID SUBJID SITEID
       AGE AGEGR1 AGEGR1N SEX RACE ETHNIC COUNTRY
       ARM ARMCD ACTARM ACTARMCD
       TRT01P TRT01PN TRT01A TRT01AN
       TRTSDT TRTEDT TRTDURD
       SAFFL ITTFL;
run;

proc sort data=adam.adsl; by STUDYID USUBJID; run;

/*----- Quick sanity log --------------------------------------------------*/
proc freq data=adam.adsl;
  tables TRT01A * SAFFL / nocum nopercent;
run;
