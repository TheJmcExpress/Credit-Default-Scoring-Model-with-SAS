************************************************
****************MASTER CODE FILE****************

    ***********************************************;
*creating library;
libname jmc "/gpfs/user_home/jcroft3/Binary";
****************************************************
***********Merging Files by on Matchkey*************

    ***************************************************;

proc contents data=cd4330.cpr;
run;

proc contents data=cd4330.perf;
run;

data CPR;
    set cd4330.cpr;
    DROP BEACON;
run;

*Removing all variables but DELQID (response variable)
*CRELIM MATCHKEY to merge data sets;

data perf;
    set cd4330.perf;
    keep delqid matchkey crelim;
run;

******Sorting Files*******;

proc sort data=cpr;
    by matchkey;
run;

proc sort data=perf;
    by matchkey delqid;
run;

data merged;
    merge perf cpr;
    by matchkey;

    if last.matchkey then
        output;
run;

data merged1;
    set merged;

    if age=. then
        delete;
    else
        age=age;
run;

data merged2;
    set merged1;

    if delqid=. then
        delete;
    else if delqid < 3 then
        goodbad=0;
    else
        goodbad=1;
run;

*****Outputting final merged data set with 1255429 Observations*****;

data jmc.FinalMerged;
    set merged2;
run;

proc freq data=jmc.finalmerged;
    tables goodbad / nocum;
run;

proc contents data=jmc.finalmerged;
run;

*****Cleaning Variable for Practice / FUN*****;

data sample;
    set cd4330.classsample;
run;

**Clearning rbal & trades**;

proc freq data=sample;
    tables rbal trades;
run;

proc means data=sample min max median;
    var rbal;
    where rbal < 140000;
run;

data clean1;
    set sample;
    median=4694.50;

    if rbal < 140000 then
        rbal=rbal;
    else
        rbal=median;
    drop median;
run;

proc means data=clean1 min max median mean;
    var trades;
run;

proc means data=clean1 min max median mean;
    var trades;
    where trades < 80;
run;

data clean1;
    set clean1;
    median=17;

    if trades < 80 then
        trades=trades;
    else
        trades=median;
    drop median;
run;

*Before Imputation;

proc univariate data=sample;
    var rbal trades;
    histogram;
run;

*After Imputation;

proc univariate data=clean1;
    var rbal trades;
    histogram;
run;

***************************************************************************;
***************Cleaning variable at scale with a macro*********************;
***************************************************************************;
*creating workign file to run through the macro

    *and dropping variables removed from preivous iterations;

data final;
    set jmc.finalmerged;
    drop DCBAL DCHIC DCRATE3 DCRATE45 DCR224 DCR4524 DCOPENB0 FFHIC FFAGE 
        FFCRATE2 FFCRATE1 FFCRATE4 FFRATE1 FFRATE2 FFRATE3 FFRATE45 FFR124 
        FFR224 FFR324 FFR4524 FFAVGMOS FFBAL75 FFBAL50 BITRADES BIOPEN BIRATE3 
        BIRATE2 BIRATE45 BIRATE79 ORTRADES OROPEN ORRATE1 ORRATE2 ORRATE45 
        ORRATE79 AFTRADES AFOPEN AFRATE1 AFRATE2 AFRATE3 AFRATE79 AFRATE45 
        CUTRADES CUOPEN CURATE79 PFTRADES PFOPEN PFRATE1 PFRATE2 PFRATE3 
        PFRATE45 PFRATE79 DCADB FFADB DCPCTSAT FFPCSAT NMPREC BIR39 ORR39 AFR39 
        CUR39 PFTR39 ORR49 AFR49 CUR49 PFR49 DCPOPEN FFPOPEN BIR29 ORR29 AFR29 
        CUR29 PFR29 DCCR1BAL FFCR1BAL FFOPENEX FFMOSOPN FFR23 OOTPTAT OAFPTAT 
        ORINQ BIOPENB0 DCMAXB FFMAXB AFMAXB ORMAXB PFMAXB FFMINB DCMINB ORMINB 
        AFMINB PFMINB DCMAXH FFMAXH ORMAXH AFMAXH PFMAXH DCMINH FFMINH ORMINH 
        AFMINH PFMINH DCADBM FFADBM ORADBM AFADBM PFADBM;
run;

%MACRO IMPV3(DSN=Final, VARS=_ALL_, EXCLUDE=crelim delqid goodbad matchkey, PCTREM=.4, 
        MSTD=4)/MINOPERATOR;
    %PUT IMPUTE 3.0 IS NOW RUNNING YOU ARE THE GREATEST;
    *DETERMING LOG STUFF;
    FILENAME LOG1 DUMMY;

    PROC PRINTTO LOG=LOG1;
    RUN;

    /*FILE AND DATA SET REFERENCES*/

    %IF %INDEX(&DSN, .) %THEN
        %DO;
            %LET LIB=%UPCASE(%SCAN(&DSN, 1, .));
            %LET DATA=%UPCASE(%SCAN(&DSN, 2, .));
        %END;
    %ELSE
        %DO;
            %LET LIB=WORK;
            %LET DATA=%UPCASE(&DSN);
        %END;
    %LET DSID=%SYSFUNC(OPEN(&LIB..&DATA));
    %LET NOBS=%SYSFUNC(ATTRN(&DSID, NOBS));
    %LET CLOSE=%SYSFUNC(CLOSE(&DSID));
    %PUT &NOBS;

    DATA TEMP;
        SET &LIB..&DATA;
    RUN;

    /*MODULE IF _ALL_ KEYWORD IS PRESENT*/

    %IF %UPCASE(&VARS)=_ALL_ AND &EXCLUDE=%THEN
        %DO;

            PROC PRINTTO ;
            RUN;

            %PUT ============================================;
            %PUT XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX;
            %PUT ;
            %PUT XXX: EXCLUDE PARAMETER IS NULL;
            %PUT XXX: IMPUTE MACRO IS TERMINATING PREMATURELY;
            %PUT;
            %PUT XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX;
            %PUT ============================================;
            %RETURN;
        %END;
    %ELSE %IF %UPCASE(&VARS)=_ALL_ AND &EXCLUDE^=%THEN
        %DO;
            %LET NEXC=%SYSFUNC(COUNTW(&EXCLUDE, %STR( )));

            /*SELECTING ALL THE VARIABLES TO BE IMPUTED*/
            PROC SQL NOPRINT;
                SELECT NAME INTO: VARNAME SEPARATED BY ' ' FROM 
                    DICTIONARY.COLUMNS WHERE UPCASE(LIBNAME)="&LIB" AND 
                    UPCASE(MEMNAME)="&DATA" AND NAME NOT 
                    IN("%SCAN(&EXCLUDE,1,%STR( ))" %DO A=2 %TO &NEXC;
                    , "%SCAN(&EXCLUDE,&A,%STR( ))" %END;
                );
            QUIT;

            %DO B=1 %TO %SYSFUNC(COUNTW(&VARNAME, %STR( )));
                %LET CURR=%SCAN(&VARNAME, &B, %STR( ));

                /*FINDING OUT IF VAR CONTAINS CODES*/
                PROC MEANS DATA=TEMP(KEEP=&CURR) NOPRINT MAX;
                    VAR &CURR;
                    OUTPUT OUT=MAX MAX=MAX;
                RUN;

                DATA _NULL_;
                    SET MAX;
                    CALL SYMPUTX('MAX', MAX);
                RUN;

                /*IF NO CODED VALUES ARE DETECTED THEN IMPUTATION OF MISSING VALUES OCCURS*/
                /*I KNOW THERE ARE MSSING VALUES WITHIN THE DATA SET BUT JUST IN CASE*/

                %IF %EVAL(%SYSFUNC(INDEXW(%STR(9999999 9999 999 99 9.9999), 
                    &MAX))<1) %THEN
                        %DO;

                        PROC SQL NOPRINT;
                            SELECT MISSING(&CURR) INTO: MISS FROM TEMP;
                        QUIT;

                        /*THIS DROPS VARS IF PROBALITY OF FINDING A MISSING VALUES IS GREATER THEN PCTREM*/

                        %IF %SYSEVALF((&MISS/&NOBS)>&PCTREM) %THEN
                            %DO;

                                PROC SQL NOPRINT;
                                    ALTER TABLE TEMP DROP &CURR;
                                QUIT;

                                PROC PRINTTO ;
                                RUN;

                                %PUT &CURR HAS BEEN REMOVED BECAUSE IT DOES NOT MEET &PCTREM CRITERION;

                                PROC PRINTTO LOG=LOG1 RUN;
                                %END;
                            %ELSE
                                %DO;

                                    /*FINDING MEDIAN*/
                                PROC MEANS DATA=TEMP(KEEP=&CURR) NOPRINT MEDIAN;
                                    VAR &CURR;
                                    OUTPUT OUT=MEDI MEDIAN=MEDIAN;
                                RUN;

                                DATA _NULL_;
                                    SET MEDI;
                                    CALL SYMPUTX('MEDIAN', MEDIAN);
                                RUN;

                                /*MISSING IMPUTATION*/
                                DATA TEMP;
                                    SET TEMP;

                                    IF &CURR=. THEN
                                        &CURR=&MEDIAN;
                                RUN;

                            %END;
                    %END;

                /*THIS NEXT PART IS SAME AS ABOVE WITH THE EXCEPTION THIS HANDLES CODED VALUES*/
%ELSE
                    %DO;

                        DATA _NULL_;
                            IF &MAX=99 THEN
                                CALL SYMPUTX('LOW', 92);
                            ELSE IF &MAX=999 THEN
                                CALL SYMPUTX('LOW', 992);
                            ELSE IF &MAX=9999 THEN
                                CALL SYMPUTX('LOW', 9992);
                            ELSE IF &MAX=9.9999 THEN
                                CALL SYMPUTX('LOW', 9.9992);
                            ELSE
                                CALL SYMPUTX('LOW', 9999992);
                        RUN;

                        PROC SQL NOPRINT;
                            SELECT COUNT(&CURR) INTO: CCODE FROM TEMP 
                                WHERE &CURR BETWEEN &LOW AND &MAX;
                            SELECT MISSING(&CURR) INTO: MISS FROM TEMP;
                        QUIT;

                        %IF %SYSEVALF(((&CCODE+&MISS)/&NOBS)>&PCTREM) %THEN
                            %DO;

                                PROC SQL ;
                                    ALTER TABLE TEMP DROP &CURR;
                                QUIT;

                                PROC PRINTTO ;
                                RUN;

                                %PUT &CURR HAS BEEN REMOVED, 
                                    TOO MUCH DATA IS CODED        OR MISSING;

                                PROC PRINTTO LOG=LOG1;
                                RUN;

                            %END;
                        %ELSE
                            %DO;

                                PROC MEANS DATA=TEMP(KEEP=&CURR) NOPRINT MEDIAN 
                                        STD MEAN;
                                    VAR &CURR;
                                    OUTPUT OUT=NUM MEDIAN=MEDIAN STD=STD 
                                        MEAN=MEAN;
                                    WHERE &CURR<&LOW;
                                RUN;

                                DATA _NULL_;
                                    SET NUM;
                                    CALL SYMPUTX('MEDIAN', MEDIAN);
                                    CALL SYMPUTX('STD', STD);
                                    CALL SYMPUTX('MEAN', MEAN);
                                RUN;

                                DATA TEMP;
                                    SET TEMP;

                                    IF &LOW<=&CURR<=&MAX | &CURR>&MEAN+&MSTD*&STD | &CURR<&MEAN-&MSTD*&STD THEN
                                        &CURR=&MEDIAN;
                                RUN;

                            %END;
                    %END;
            %END;
        %END;

    /*THIS NEXT PART HANDLES A LIST OF VARIABLES PROVIDED IN THE VAR STATEMENT*/
%ELSE
        %DO;
            %LET NVAR=%SYSFUNC(COUNTW(&VARS, %STR( )));

            %DO C=1 %TO &NVAR;
                %LET CURR=%SCAN(&VARS, &C, %STR( ));

                PROC MEANS DATA=TEMP(KEEP=&CURR) NOPRINT MAX;
                    VAR &CURR;
                    OUTPUT OUT=MAX MAX=MAX;
                RUN;

                DATA _NULL_;
                    SET MAX;
                    CALL SYMPUTX('MAX', MAX);
                RUN;

                %IF %EVAL(%SYSFUNC(INDEXW(%STR(9999999 9999 999 99 9.9999), 
                    &MAX))<1) %THEN
                        %DO;

                        PROC SQL NOPRINT;
                            SELECT MISSING(&CURR) INTO: MISS FROM TEMP;
                        QUIT;

                        %IF %SYSEVALF((&MISS/&NOBS)>&PCTREM) %THEN
                            %DO;

                                PROC SQL NOPRINT;
                                    ALTER TABLE TEMP DROP &CURR;
                                QUIT;

                            %END;
                        %ELSE
                            %DO;

                                PROC MEANS DATA=TEMP(KEEP=&CURR) NOPRINT MEDIAN;
                                    VAR &CURR;
                                    OUTPUT OUT=MEDI MEDIAN=MEDIAN;
                                RUN;

                                DATA _NULL_;
                                    SET MEDI;
                                    CALL SYMPUTX('MEDIAN', MEDIAN);
                                RUN;

                                DATA TEMP;
                                    SET TEMP;

                                    IF &CURR=. THEN
                                        &CURR=&MEDIAN;
                                RUN;

                            %END;
                    %END;
                %ELSE
                    %DO;

                        DATA _NULL_;
                            IF &MAX=99 THEN
                                CALL SYMPUTX('LOW', 92);
                            ELSE IF &MAX=999 THEN
                                CALL SYMPUTX('LOW', 992);
                            ELSE IF &MAX=9999 THEN
                                CALL SYMPUTX('LOW', 9992);
                            ELSE IF &MAX=9.9999 THEN
                                CALL SYMPUTX('LOW', 9.9992);
                            ELSE
                                CALL SYMPUTX('LOW', 9999992);
                        RUN;

                        PROC SQL NOPRINT;
                            SELECT COUNT(&CURR) INTO: CCODE FROM TEMP 
                                WHERE &CURR BETWEEN &LOW AND &MAX;
                            SELECT MISSING(&CURR) INTO: MISS FROM TEMP;
                        QUIT;

                        %IF %SYSEVALF(((&CCODE+&MISS)/&NOBS)>&PCTREM) %THEN
                            %DO;

                                PROC SQL ;
                                    ALTER TABLE TEMP DROP &CURR;
                                QUIT;

                                PROC PRINTTO ;
                                RUN;

                                %PUT &CURR HAS BEEN REMOVED, 
                                    TOO MUCH DATA IS CODED        OR MISSING;

                                PROC PRINTTO LOG=LOG1;
                                RUN;

                            %END;
                        %ELSE
                            %DO;

                                PROC MEANS DATA=TEMP(KEEP=&CURR) NOPRINT MEDIAN 
                                        STD MEAN;
                                    VAR &CURR;
                                    OUTPUT OUT=NUM MEDIAN=MEDIAN STD=STD 
                                        MEAN=MEAN;
                                    WHERE &CURR<&LOW;
                                RUN;

                                DATA _NULL_;
                                    SET NUM;
                                    CALL SYMPUTX('MEDIAN', MEDIAN);
                                    CALL SYMPUTX('STD', STD);
                                    CALL SYMPUTX('MEAN', MEAN);
                                RUN;

                                DATA TEMP;
                                    SET TEMP;

                                    IF &LOW<=&CURR<=&MAX | &CURR>(&MEAN + &MSTD*&STD) 
                                        | &CURR<(&MEAN -&MSTD*&STD) THEN
                                            &CURR=&MEDIAN;
                                RUN;

                            %END;
                    %END;
            %END;
        %END;

    /*CREATING NEW DATASET*/
    DATA &LIB..&DATA.OUT;
        SET TEMP;
    RUN;

    PROC DATASETS NOLIST;
        DELETE NUM TEMP MEDI MAX;
    QUIT;

    PROC PRINTTO ;
    RUN;

    %PUT THIS MACRO HAS FINISHED RUNNING HAVE A NICE DAY;
%MEND IMPV3;

%IMPV3(DSN=Final, VARS=_ALL_, EXCLUDE=crelim delqid goodbad , PCTREM=.4, 
    MSTD=4);

proc contents data=finalout;
run;

*Master file below does not include credit limit;
*Creating Master data file after running macro;

data jmc.master;
    set finalout;
run;

proc contents data=jmc.master;
run;

/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/**/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/


        /*Variable Clustering as an option for Variable reduction;
*/*/*/*/*/*/*/


        /*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/**/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/*/;

data master;
    set jmc.master;
    drop BEACON DELQID CRELIM MATCHKEY GOODBAD AGE;
run;

%let inputs = _ALL_;
ods listing close;
ods output clusterquality=summary rsquare=clusters;

proc varclus data=master maxclusters=44;
    var &inputs;
run;

ods listing;
*If no maxclusters options specified, SAS optimizes at 33 clusters;

data _null_;
    set summary;
    call symput ('nvar', compress(NumberofClusters));
run;

*Below set NumberofCluster = to whatever I choose to get the cluster grouping output;

proc print data=clusters noobs;
    where NumberofClusters=44;
    var _ALL_;
run;

*run a proc corr manually for variables in clusters 1 and 2 of cluster I choose;

proc corr data=master;
    var DCCRATE7 DCRATE79 DCR7924 DCN90P24 DCR39P24 DCCR49 BRTRADES BROPEN 
        BRCRATE1 BRRATE1 BRR124 BROPENEX;
run;

****************************************************


        *Creating checkoint copy of Master file precluster;
*data jmc.master_precluster;
*	set ;
*run;
******************************************************;
*Creating data set with reduced 44 possible predictors plus 4 variables 
removed for cluster analysis;

data MasterCluster;
    set jmc.master;
    KEEP DELQID CRELIM MATCHKEY GOODBAD DCRATE79 BRCRATE1 BRCRATE7 TOPEN12 
        TRCR49 FFCR49 RBAL BRR324 BRAGE BRR4524 DCOPENEX TOPENB75 BRRATE2 
        TROPENEX BNKINQS LOCINQS BADPR1 TCR1BAL FFN90P24 BRR39P24 CRATE2 TOPEN3 
        BRCRATE3 DCAGE BRHIC TOPEN RADB6 COLLS DCR39 TPCTSAT BRMINB BKPOP 
        DCLAAGE LAAGE FFTRADES FFWCRATE FFR29P24 FFLAAGE ORRATE3 BKP BRCRATE4 
        FININQS DCWCRATE OBRPTAT;
run;

proc reg data=mastercluster;
    model matchkey=DCRATE79 BRCRATE1 BRCRATE7 TOPEN12 TRCR49 FFCR49 RBAL BRR324 
        BRAGE BRR4524 DCOPENEX TOPENB75 BRRATE2 TROPENEX BNKINQS LOCINQS BADPR1 
        TCR1BAL FFN90P24 BRR39P24 CRATE2 TOPEN3 BRCRATE3 DCAGE BRHIC TOPEN 
        RADB6 COLLS DCR39 TPCTSAT BRMINB BKPOP DCLAAGE LAAGE FFTRADES FFWCRATE 
        FFR29P24 FFLAAGE ORRATE3 BKP BRCRATE4 FININQS DCWCRATE OBRPTAT / VIF;
    run;

data jmc.master;
    set mastercluster;
run;

proc contents data=jmc.master;
run;