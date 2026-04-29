/******************************************************************************
 * Program  : adlbc.sas
 * Purpose  : Derive ADLBC (Laboratory Chemistry, Basic Data Structure)
 *            per ADaM IG v1.3 §4 (BDS).
 * Inputs   : SDTM.LB, ADAM.ADSL
 * Output   : ADAM.ADLBC
 * Notes    : One record per USUBJID per PARAMCD per AVISIT.
 *            ABLFL = "Y" on the last non-missing record on/before TRTSDT.
 *            CHG and PCHG are derived from the baseline AVAL.
 *****************************************************************************/

%include "setup.sas";

proc sql;
  create table _lb as
    select l.STUDYID, l.USUBJID, l.LBSEQ,
           l.LBTESTCD as PARAMCD,
           l.LBTEST   as PARAM,
           l.LBSTRESN as AVAL,
           l.LBSTRESC as AVALC,
           l.LBSTRESU as AVALU,
           l.VISIT    as AVISIT,
           l.VISITNUM as AVISITN,
           input(l.LBDTC, ?? e8601da.) as ADT format=date9.,
           s.TRTSDT, s.TRTEDT, s.TRT01A, s.SAFFL
    from sdtm.lb l
         left join adam.adsl s
           on l.STUDYID = s.STUDYID and l.USUBJID = s.USUBJID
    where l.LBCAT = "CHEMISTRY";
quit;

proc sort data=_lb; by STUDYID USUBJID PARAMCD ADT; run;

/*----- Baseline flag: last record on/before TRTSDT ----------------------*/
data _bl;
  set _lb;
  by STUDYID USUBJID PARAMCD;
  retain _last_pre _last_aval;
  if first.PARAMCD then do; _last_pre = .; _last_aval = .; end;
  if not missing(ADT) and not missing(TRTSDT) and ADT <= TRTSDT
     and not missing(AVAL) then do;
    _last_pre  = ADT;
    _last_aval = AVAL;
  end;
  if last.PARAMCD then output;
  keep STUDYID USUBJID PARAMCD _last_pre _last_aval;
run;

data adam.adlbc (label="Laboratory Test Results - Chemistry");
  merge _lb (in=in_lb)
        _bl (rename=(_last_pre=BLDT _last_aval=BASE));
  by STUDYID USUBJID PARAMCD;

  length ABLFL $1;
  if not missing(ADT) and not missing(BLDT) and ADT = BLDT and AVAL = BASE
    then ABLFL = "Y";

  if not missing(AVAL) and not missing(BASE) then do;
    CHG  = AVAL - BASE;
    if BASE ne 0 then PCHG = (CHG / BASE) * 100;
  end;

  keep STUDYID USUBJID PARAMCD PARAM AVAL AVALC AVALU
       AVISIT AVISITN ADT ABLFL BASE CHG PCHG
       TRTSDT TRT01A SAFFL;
run;

proc sort data=adam.adlbc; by STUDYID USUBJID PARAMCD AVISITN; run;
