/* ---------------------------------------------------------------------------
   00  One-time setup for SAS OnDemand for Academics
   Creates the folders the legacy SAS jobs expect under your home directory:
       ~/exl/input     upload the 4 CSVs from legacy/golden/sas_input/ here
       ~/exl/programs  upload J04, J07, J10 and 99_run_all.sas here
       ~/exl/lib       SAS library the jobs write to
       ~/exl/golden    golden CSVs the jobs export (download these afterwards)
   Safe to run more than once.
--------------------------------------------------------------------------- */
%let home = /home/&sysuserid;

data _null_;
    length rc $256;
    rc = dcreate('exl',      "&home");
    rc = dcreate('input',    "&home/exl");
    rc = dcreate('programs', "&home/exl");
    rc = dcreate('lib',      "&home/exl");
    rc = dcreate('golden',   "&home/exl");
run;

%put NOTE: Folders ready under &home/exl (input, programs, lib, golden).;
