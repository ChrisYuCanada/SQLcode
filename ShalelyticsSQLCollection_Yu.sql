# eliminate the duplicate raws in the completion and production tables ----It does not work to me for now???!!!!
CREATE Table bcnorth_mntn_completb like bcnorth_mntn_complet;
Alter table bcnorth_mntn_completb add constraint complet_dedu unique(uwi, datte, typecode, uprdpth_m, lwrdpth_m, trtmntcode);
INSERT IGNORE INTO bcnorth_mntn_completb SELECT * FROM bcnorth_mntn_complet;
RENAME TABLE bcnorth_mntn_complet TO old_bcnorth_mntn_complet, bcnorth_mntn_completb TO bcnorth_mntn_complet;
DROP TABLE bcnorth_mntn_completb;

# create mother table of shale analysis
CREATE table bcnorth_mntn_shalefactor (
	SortUWI char(25) unique NOT NULL, 
    UWI char(25) unique NOT NULL,
    Operator char(45),
    Fluid char(25),
    OnProdDate DATE,
    CumPrd6mnth DOUBLE,
    CumPrd12mnth DOUBLE,
    LatLength_m DOUBLE,
    FracStages INT(11),
    ClustPerStage INT(11),
    ShotPerM INT(11));

# genertae the shale play analysis factor table
INSERT INTO shalelytics.shale_factors(SortUWI, UWI, OnPrdDate)
SELECT SortUWI, UWI, OnPrdDate
FROM shalelytics.bcsouth_mntn_prdsumry;

# create bbl-oil-equavlent for acumulative production in month
UPDATE shalelytics.bcsouth_mntn_prd SET CumPrd_boe = ROUND(CumPrdGAS_mcf/6 + CumPrdOil_bbl + CumPrdCond_bbl, 2);

# collect 6 month cumulative production from Production table to shale factor table
UPDATE shalelytics.shale_factors
	SET shalelytics.shale_factors.CumPrd6mnth = (
    SELECT bcsouth_mntn_prd.CumPrd_boe
		FROM shalelytics.bcsouth_mntn_prd
        WHERE shalelytics.bcsouth_mntn_prd.SortUWI = shalelytics.shale_factors.SortUWI
			AND shalelytics.bcsouth_mntn_prd.PrdMonth = 6);

/* delete production months fewer than 12 months */
DELETE FROM shalelytics.shale_factors WHERE CumPrd6mnth IS NULL;
DELETE FROM shalelytics.shale_factors WHERE CumPrd12mnth IS NULL;

# compute horizontal well lateral length
UPDATE shalelytics.shale_factors
	SET shalelytics.shale_factors.LatLength_m = (
    SELECT ROUND(max(bcsouth_mntn_completion.LwrDpth_m) - min(bcsouth_mntn_completion.UprDpth_m), 2)
		FROM shalelytics.bcsouth_mntn_completion
        WHERE shalelytics.bcsouth_mntn_completion.SortUWI = shalelytics.shale_factors.SortUWI);

# reduce the rows from completion table to the only wells included in the shale factor table       
DELETE FROM shalelytics.bcsouth_mntn_completion
WHERE shalelytics.bcsouth_mntn_completion.SortUWI NOT IN (Select shalelytics.shale_factors.SortUWI FROM shalelytics.shale_factors);

# compute the Frac stage from completion table for fill in the shale factor table
UPDATE shalelytics.shale_factors
	SET shalelytics.shale_factors.FracStages = (
    SELECT COUNT(*)
		FROM shalelytics.bcsouth_mntn_completion
        WHERE shalelytics.bcsouth_mntn_completion.SortUWI = shalelytics.shale_factors.SortUWI
			AND shalelytics.bcsouth_mntn_completion.TrtmntCode = 41 -- 41 is Frac treatment
            AND shalelytics.bcsouth_mntn_completion.LwrDpth_m - shalelytics.bcsouth_mntn_completion.UprDpth_m < 400); 
            
# ADD: elimate light frac process (frac space 0.1m to 15m per stage, they do not fit in the modern Mutistage Frac Bucket) using Frac interval < 25m
UPDATE shalelytics.shale_factors
	SET shalelytics.shale_factors.FracStages = (
    SELECT COUNT(*)
		FROM shalelytics.bcsouth_mntn_completion
        WHERE shalelytics.bcsouth_mntn_completion.SortUWI = shalelytics.shale_factors.SortUWI
			AND shalelytics.bcsouth_mntn_completion.TrtmntCode = 41 -- 41 is Frac treatment
            AND shalelytics.bcsouth_mntn_completion.LwrDpth_m - shalelytics.bcsouth_mntn_completion.UprDpth_m < 400
            AND shalelytics.bcsouth_mntn_completion.LwrDpth_m - shalelytics.bcsouth_mntn_completion.UprDpth_m > 25);

# compute Frac Space in shale factor table
UPDATE shalelytics.shale_factors
	SET FracSpace_m = round(LatLength_m/FracStages, 1);
    
# compute or fetch clusters per fracstage and bullets per meter for the shale factor table
UPDATE shalelytics.shale_factors
	SET shalelytics.shale_factors.ClustsPerStage = (
		SELECT COUNT(*)
		FROM shalelytics.bcsouth_mntn_completion
        WHERE shalelytics.bcsouth_mntn_completion.SortUWI = shalelytics.shale_factors.SortUWI
			AND shalelytics.bcsouth_mntn_completion.TypeCode = 2)/shalelytics.shale_factors.FracStages,
		shalelytics.shale_factors.ShotPerM = (
		SELECT shalelytics.bcsouth_mntn_completion.ShotPerM 
        FROM shalelytics.bcsouth_mntn_completion
        WHERE shalelytics.bcsouth_mntn_completion.SortUWI = shalelytics.shale_factors.SortUWI
        GROUP BY shalelytics.bcsouth_mntn_completion.ShotPerM
        ORDER BY COUNT(*) DESC
        LIMIT 1);
        
        
# insert a new column
alter table shale.frac
add column uwi char(25) NOT NULL AFTER wa;

# match engineering UWI in frac and Perf tables to Geological UWI by inserting symbles "-" "\" and taking uot extra 0
update shale.frac
set	uwi = insert (uwi,4,0,'/');
update shale.frac
set	uwi = insert (uwi,7,0,'-');
update shale.frac
set	uwi = insert (uwi,11,0,'-');
update shale.frac
set	uwi = insert (uwi,15,0,'-');
update shale.frac
set	uwi = insert (uwi,19,0,'/');
update shale.frac
set	uwi = insert (uwi,20,1,'');

# create an analytics well list
Create table analy_well_lst (
	wa INT unique not null,
    uwi char(25) unique not null,
    on_prd_date date
) Engine=INNODB;

# eliminate non letter and non number function "alphanum"
DROP FUNCTION IF EXISTS alphanum; 
DELIMITER | 
CREATE FUNCTION alphanum( str CHAR(255) ) RETURNS CHAR(255) DETERMINISTIC
BEGIN 
  DECLARE i, len SMALLINT DEFAULT 1; 
  DECLARE ret CHAR(255) DEFAULT ''; 
  DECLARE c CHAR(1); 
  SET len = CHAR_LENGTH( str ); 
  REPEAT 
    BEGIN 
      SET c = MID( str, i, 1 ); 
      IF c REGEXP '[[:alnum:]]' THEN 
        SET ret=CONCAT(ret,c); 
      END IF; 
      SET i = i + 1; 
    END; 
  UNTIL i > len END REPEAT; 
  RETURN ret; 
END | 
DELIMITER ; 

Update mntn_prd_smry
SET bridg_uwi = alphanum(uwi);

# update frac with uwi added on
UPDATE frac
	inner join mntn_prd_smry on frac.bridg_uwi = mntn_prd_smry.bridg_uwi
SET frac.uwi = mntn_prd_smry.uwi
where frac.bridg_uwi = mntn_prd_smry.bridg_uwi;

# convert gas to boe and sum al the hydrocarbon productions in prd table
UPDATE analy_well_lst
	INNER JOIN mntn_prd On analy_well_lst.wa = mntn_prd.wa
SET analy_well_lst.cum_6mn_prd = mntn_prd.cum_cond + mntn_prd.cum_oil + mntn_prd.cum_gas/6
where mntn_prd.mn_count = 5;

# copy a table
create table perfbackup like perf;
INSERT INTO perfbackup (SELECT * FROM perf);

# update a table with columns generated from other table
UPDATE gr_well_list
	JOIN frac On gr_well_list.wa = frac.wa
	SET gr_well_list.frac_top = (Select min(compltn_tp_dep) from frac where gr_well_list.wa=frac.wa),
		gr_well_list.frac_base = (select max(frac.compltn_bt_dep) from frac where gr_well_list.wa=frac.wa);