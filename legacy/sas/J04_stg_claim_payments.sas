/* ---------------------------------------------------------------------------
   J04  Claim payment staging                  schedule: nightly 01:40
   Source : claim_payments.csv  (payments system extract)
   Target : legacy.stg_claim_payments  (+ golden CSV)
   Note   : extract can repeat payments; keep one row per payment_id.
--------------------------------------------------------------------------- */
%let root = /home/&sysuserid/exl;      /* SAS OnDemand home folder */
libname legacy "&root/lib";
options missing=' ';

data work.claim_payments;
    infile "&root/input/claim_payments.csv" dsd firstobs=2 truncover;
    length payment_id $12 claim_id $12 payment_type $12;
    informat payment_date yymmdd10.;
    format   payment_date yymmdd10.;
    input payment_id $ claim_id $ payment_date payment_type $ amount;
run;

proc sort data=work.claim_payments out=work.dedup nodupkey;
    by payment_id;
run;

data legacy.stg_claim_payments;
    set work.dedup;
    length amount_band $6;
    if amount < 100 then amount_band = 'SMALL';
    else if amount < 1000 then amount_band = 'MEDIUM';
    else amount_band = 'LARGE';
run;

proc export data=legacy.stg_claim_payments
    outfile="&root/golden/stg_claim_payments.csv" dbms=csv replace;
run;
