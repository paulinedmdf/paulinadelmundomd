/******************************************************************************
 * Program  : adae.sas
 * Purpose  : Derive ADAE (Adverse Events analysis dataset) per
 *            ADaMIG-OCCDS v1.1.
 * Inputs   : SDTM.AE, ADAM.ADSL
 * Output   : ADAM.ADAE
 * Notes    : Occurrence-level structure (one record per AE per subject).
 *            TRTEMFL is the treatment-emergent flag — gated on AESTDT
 *            relative to TRTSDT/TRTEDT.
 *****************************************************************************/

%include "setup.sas";

proc sql;
  create table _ae as
    select a.*,
           input(a.AESTDTC, ?? e8601da.) as AESTDT format=date9.,
           input(a.AEENDTC, ?? e8601da.) as AEENDT format=date9.,
           s.TRTSDT, s.TRTEDT, s.TRT01A, s.SAFFL
    from sdtm.ae a
         left join adam.adsl s
           on a.STUDYID = s.STUDYID and a.USUBJID = s.USUBJID;
quit;

data adam.adae (label="Adverse Events Analysis Dataset");
  set _ae;

  /* Treatment-emergent: AE start on/after TRTSDT and on/before TRTEDT+30d */
  length TRTEMFL $1;
  if not missing(AESTDT) and not missing(TRTSDT) then do;
    if AESTDT >= TRTSDT and (missing(TRTEDT) or AESTDT <= TRTEDT + 30)
      then TRTEMFL = "Y";
    else TRTEMFL = "N";
  end;

  /* Analysis study day */
  if not missing(AESTDT) and not missing(TRTSDT) then
    ASTDY = AESTDT - TRTSDT + (AESTDT >= TRTSDT);
  if not missing(AEENDT) and not missing(TRTSDT) then
    AENDY = AEENDT - TRTSDT + (AEENDT >= TRTSDT);

  keep STUDYID USUBJID AESEQ
       AETERM AEDECOD AEBODSYS
       AESEV AESER AEREL AEOUT
       AESTDT AEENDT ASTDY AENDY
       TRT01A TRTEMFL SAFFL;
run;

proc sort data=adam.adae; by STUDYID USUBJID AESEQ; run;
