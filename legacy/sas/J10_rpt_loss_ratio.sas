/* ---------------------------------------------------------------------------
   J10  Loss ratio by product and year         schedule: monthly, BD4 08:00
   Source : legacy.claim_totals (J07), stg_policies.csv (J02 export),
            stg_policy_earned_premium.csv (J06 export)
   Target : legacy.rpt_loss_ratio  (+ golden CSV)
   Rule   : incurred indemnity by loss year / earned premium by policy year
   Used by: Actuarial pricing review
--------------------------------------------------------------------------- */
%let root = /home/&sysuserid/exl;
libname legacy "&root/lib";
options missing=' ';

data work.policies;
    infile "&root/input/stg_policies.csv" dsd firstobs=2 truncover;
    length policy_id $12 customer_id $10 product_code $10 line_of_business $10
           broker_id $10 channel $10 rolling_flag $1 status $20 payment_frequency $10;
    informat start_date end_date yymmdd10.;
    format   start_date end_date yymmdd10.;
    input policy_id $ customer_id $ product_code $ line_of_business $ broker_id $
          channel $ start_date end_date rolling_flag $ status $ annual_premium
          payment_frequency $;
run;

data work.earned_policy;
    infile "&root/input/stg_policy_earned_premium.csv" dsd firstobs=2 truncover;
    length policy_id $12 product_code $10;
    input policy_id $ product_code $ policy_year days_in_force earned_premium;
run;

proc sql;
    create table work.earned as
    select product_code, policy_year, sum(earned_premium) as earned_premium
    from work.earned_policy
    group by product_code, policy_year;

    create table work.incurred as
    select p.product_code,
           year(t.loss_date)     as loss_year,
           sum(t.indemnity_paid) as incurred,
           count(*)              as claim_count
    from legacy.claim_totals t
    inner join work.policies p
      on t.policy_id = p.policy_id
    group by p.product_code, calculated loss_year;

    create table work.lr as
    select coalesce(e.product_code, i.product_code) as product_code length=10,
           coalesce(e.policy_year, i.loss_year)     as year,
           e.earned_premium,
           i.incurred,
           i.claim_count
    from work.earned e
    full join work.incurred i
      on e.product_code = i.product_code
     and e.policy_year  = i.loss_year;
quit;

data legacy.rpt_loss_ratio;
    set work.lr;
    if earned_premium > 0 then loss_ratio = round(incurred / earned_premium, 0.0001);
run;

proc export data=legacy.rpt_loss_ratio
    outfile="&root/golden/rpt_loss_ratio.csv" dbms=csv replace;
run;
