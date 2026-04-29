/******************************************************************************
 * Program  : t_14_2_01_demog.sas
 * Purpose  : Table 14-2.01 — Demographic and Baseline Characteristics
 *            (ICH E3 §14.2.1), safety population, by treatment arm.
 * Inputs   : ADAM.ADSL
 * Output   : output/t_14_2_01_demog.rtf
 *****************************************************************************/

%include "setup.sas";

ods listing close;
ods rtf file="output/t_14_2_01_demog.rtf" style=journal;

title1 "CDISC Pilot 01 — Donepezil in Mild to Moderate Alzheimer's Disease";
title2 "Table 14-2.01  Demographic and Baseline Characteristics  (Safety Population)";

proc tabulate data=adam.adsl (where=(SAFFL = "Y")) format=8.1 missing;
  class TRT01A AGEGR1 SEX RACE;
  var   AGE;
  table (AGEGR1='Age group (years)'  all='Total')
        (SEX  ='Sex'                 all='Total')
        (RACE ='Race'                all='Total'),
        (TRT01A='' all='Overall') * (n='n' colpctn='%' * f=5.1)
        AGE  =' ' * (mean='Mean' std='SD' median='Median' min='Min' max='Max');
run;

ods rtf close;
ods listing;
