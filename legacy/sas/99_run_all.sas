/* ---------------------------------------------------------------------------
   99  Run the three legacy SAS jobs in schedule order and check the results
       J04 (claim payments) -> J07 (claim totals) -> J10 (loss ratio)
   Before running: 00_setup_folders.sas has been run, the 4 input CSVs are in
   ~/exl/input and the three J*.sas programs are in ~/exl/programs.
   After running: download every CSV in ~/exl/golden into legacy/golden/.
--------------------------------------------------------------------------- */
%let root = /home/&sysuserid/exl;

%macro need(path);
    %if not %sysfunc(fileexist(&path)) %then %do;
        %put ERROR: Missing &path - upload it before running this program.;
        %abort cancel;
    %end;
%mend;

%need(&root/input/claim_payments.csv)
%need(&root/input/stg_claims.csv)
%need(&root/input/stg_policies.csv)
%need(&root/input/stg_policy_earned_premium.csv)
%need(&root/programs/J04_stg_claim_payments.sas)
%need(&root/programs/J07_stg_claim_totals.sas)
%need(&root/programs/J10_rpt_loss_ratio.sas)

%include "&root/programs/J04_stg_claim_payments.sas";
%include "&root/programs/J07_stg_claim_totals.sas";
%include "&root/programs/J10_rpt_loss_ratio.sas";

/* Row counts, to compare with what the factory produces */
libname legacy "&root/lib";
title "Legacy SAS outputs";
proc sql;
    select 'stg_claim_payments' as table length=20, count(*) as rows from legacy.stg_claim_payments
    union all
    select 'claim_totals',       count(*) from legacy.claim_totals
    union all
    select 'rpt_loss_ratio',     count(*) from legacy.rpt_loss_ratio;
quit;
title;

%need(&root/golden/stg_claim_payments.csv)
%need(&root/golden/claim_totals.csv)
%need(&root/golden/rpt_loss_ratio.csv)
%put NOTE: All 3 golden CSVs written to &root/golden - download them into legacy/golden/.;
