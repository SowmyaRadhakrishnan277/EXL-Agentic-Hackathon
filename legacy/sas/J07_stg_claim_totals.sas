/* ---------------------------------------------------------------------------
   J07  Paid totals per claim                  schedule: nightly 02:10
   Source : legacy.stg_claim_payments (J04), stg_claims.csv (J05 export)
   Target : legacy.claim_totals  (+ golden CSV)
--------------------------------------------------------------------------- */
%let root = /home/&sysuserid/exl;
libname legacy "&root/lib";
options missing=' ';

data work.stg_claims;
    infile "&root/input/stg_claims.csv" dsd firstobs=2 truncover;
    length claim_id $12 policy_id $12 claim_type $20 status $20;
    informat loss_date reported_date yymmdd10.;
    format   loss_date reported_date yymmdd10.;
    input claim_id $ policy_id $ loss_date reported_date claim_type $ status $
          reserve_amount claim_age_days;
run;

proc sql;
    create table work.pay_by_claim as
    select claim_id,
           sum(case when payment_type = 'INDEMNITY' then amount end) as indemnity_paid,
           sum(case when payment_type = 'EXPENSE'   then amount end) as expense_paid,
           count(*)                                                  as payment_count
    from legacy.stg_claim_payments
    group by claim_id;

    create table work.joined as
    select c.claim_id, c.policy_id, c.claim_type, c.loss_date,
           p.indemnity_paid, p.expense_paid,
           coalesce(p.payment_count, 0) as payment_count
    from work.stg_claims c
    left join work.pay_by_claim p
      on c.claim_id = p.claim_id;
quit;

data legacy.claim_totals;
    set work.joined;
    length small_claim_flag $1;
    total_paid = round(indemnity_paid + expense_paid, 0.01);
    if total_paid < 500 then small_claim_flag = 'Y';
    else small_claim_flag = 'N';
run;

proc export data=legacy.claim_totals
    outfile="&root/golden/claim_totals.csv" dbms=csv replace;
run;
