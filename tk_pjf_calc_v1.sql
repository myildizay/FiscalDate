/*
================================================================================
--
-- File name:   tk_pjf_calc.sql
--
-- Purpose:     Compute Sell-Out weighted projections for Turkey.
--
-- Author:      Rafal Koelner (rkoelner@pl.imshealth.com)
--
-- Usage:       To be embedded within production pipeline as an SSIS package.
--              It can be triggered manually (temp tables cleanup required).
--              Please consider changes to the temp table names, and in
--              particular changes to %Oper and %Ctrl tables (these should be
--              persistent). Also, three extra files from the Stats Office
--              provided annually might need to be revised.
--
--              Overall, below 5 tables used in the code might need some
--              refactoring/revision:
--              [dbo].[tmpPjfCtrl] (param varchar(30), val varchar(100))
--              [dbo].[tmpPjfPdOper] (PdID int)
--              [dbo].[tmpPjfProdFlag] (...) -> provided by SO
--              [dbo].[tmpPjfPFG] (...)      -> provided by SO
--
-- Parameters:
--              The only parameter is a single month or a list of months
--              inserted into dbo.[tmpPjfPdOper] table using YYYYXX format.
--
-- Other:       The engine computes projections for each
--              {Period x Shop x Pack} that exists on the Panel.
--              ... fill in details ...
--              ... fill in details ...
--              ... fill in details ...
--
--             Analytic functions were removed... They don't perform well.
--
-- Version:    1.0
--
================================================================================
*/

--------------------------------------------------------------------------------
-- STEP#0 Full cleanup
-- Note:
-- drop table dbo.tmpPjf_010_Pd;
-- drop table dbo.tmpPjf_021_ShopsBase;
-- drop table dbo.tmpPjf_022_ShopsWx;
-- drop table dbo.tmpPjf_023_ShopsWxOrphan;
-- drop table dbo.tmpPjf_024_ShopsUnvrs;
-- drop table dbo.tmpPjf_025_ShopsSorted;
-- drop table dbo.tmpPjf_026_ShopsCUM;
-- drop table dbo.tmpPjf_027_Shops;
-- drop table dbo.tmpPjf_030_Packs;
-- drop table dbo.tmpPjf_040_Unvrs;
-- drop table dbo.tmpPjf_050_Panel;
-- drop table dbo.tmpPjf_060_UnvrsWxBase;
-- drop table dbo.tmpPjf_061_UnvrsWxClass;
-- drop table dbo.tmpPjf_062_UnvrsWxBrand;
-- drop table dbo.tmpPjf_063_UnvrsWxATC4;
-- drop table dbo.tmpPjf_064_UnvrsWxATC3;
-- drop table dbo.tmpPjf_065_UnvrsWxATC2;
-- drop table dbo.tmpPjf_066_UnvrsWxATC1;
-- drop table dbo.tmpPjf_067_UnvrsWxBasket;
-- drop table dbo.tmpPjf_068_UnvrsWxRx;
-- drop table dbo.tmpPjf_069_UnvrsWx;
-- drop table dbo.tmpPjf_070_PanelWxBase;
-- drop table dbo.tmpPjf_071_PanelWxClass;
-- drop table dbo.tmpPjf_072_PanelWxBrand;
-- drop table dbo.tmpPjf_073_PanelWxATC4;
-- drop table dbo.tmpPjf_074_PanelWxATC3;
-- drop table dbo.tmpPjf_075_PanelWxATC2;
-- drop table dbo.tmpPjf_076_PanelWxATC1;
-- drop table dbo.tmpPjf_077_PanelWxBasket;
-- drop table dbo.tmpPjf_078_PanelWxRx;
-- drop table dbo.tmpPjf_079_PanelWx;
-- drop table dbo.tmpPjf_080_Potential;
-- drop table dbo.tmpPjf_090_Orphans;
-- drop table dbo.tmpPjf_091_OrphansWxClass;
-- drop table dbo.tmpPjf_092_OrphansWxBrand;
-- drop table dbo.tmpPjf_093_OrphansWxATC4;
-- drop table dbo.tmpPjf_094_OrphansWxATC3;
-- drop table dbo.tmpPjf_095_OrphansWxATC2;
-- drop table dbo.tmpPjf_096_OrphansWxATC1;
-- drop table dbo.tmpPjf_097_OrphansWxBasket;
-- drop table dbo.tmpPjf_098_OrphansWxRx;
-- drop table dbo.tmpPjf_099_OrphansWx;
-- drop table dbo.tmpPjf_100_Pjf;

-- Pre-proc:
--
-- 1) Current 'process control' parameters:
-- select * from [dbo].[tmpPjfCtrl]
-- param    val
-- -------  ------
-- MthsAvg  9       -->> Only this one is used at the moment!
--
-- 1) Which period to project? E.g.
-- select * from [dbo].[tmpPjfPdOper]
-- PdID
-- ------
-- 201707
-- ^^^ insert whatever period you want to project ^^^

--------------------------------------------------------------------------------
-- STEP#1 Periods
-- Note:
-- In this step period information is collected based on the input parameters
-- stored in dbo.[tmpPjfPdOper] and Sell-In data. For each month processed a
-- number of months (controlled through dbo.[tmpPjfCtrl]) is generated in
-- YYYYMM format, if they exist in wholesale fact. Also, each period is assigned
-- a monotonic number, which comes in handy to make use of analytical functions
-- for computing avarages by RANGE, but this will not apply for Turkey, since
-- all analytic functions are gone from the code...

-- drop table dbo.tmpPjf_010_Pd;

with
Ctrl as (
  select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg'
),
Unvrs as (
  select distinct Period as PdID from IMSDAS_PROJECT_TURKEY_PHARMATREND_MONTHLY.dbo.BackSellInData
),
Perds as (
  select (cast (substring(PdID,1,4) as int) - cast (substring(MinPdID,1,4) as int))*12+ cast (substring(PdID,5,2) as int) as PdNbr, PdID
    from ( select PdID, min(PdID) over() as MinPdID from Unvrs ) a
)
select p1.PdNbr, p1.PdID, p2.PdNbr as ProcPdNbr, PdOper.PdID as ProcPdID
  into dbo.tmpPjf_010_Pd
  from Perds p1
  cross join Ctrl
  left outer join dbo.tmpPjfPdOper PdOper
    on convert(datetime,concat(p1.PdID,'01')) between dateadd(month, -MthsAvg, convert(datetime,concat(PdOper.PdID,'01'))) and dateadd(month, -1, convert(datetime,concat(PdOper.PdID,'01')))
 inner join Perds p2
    on PdOper.PdID = p2.PdID
 where PdOper.PdID is not null
;

--------------------------------------------------------------------------------
-- STEP#2 Shops
-- Note:
-- Collect outlet references for all shops in the Universe and assign TSC.
-- Each shop is assigned to a 'small', 'medium', 'large' turnover size class
-- based on Sell-In volume gathered for the previous full year to the currently
-- projected year using cumulative sum so that:
-- 20% of total turnover - small pharmacies
-- 30% of total turnover - medium pharmacies
-- 50% of total turnover - large pharmacies

-- drop table dbo.tmpPjf_021_ShopsBase;

-- Prepare Universe
-- !!!!!!!!
-- PANEL RESULTS SHOULD BE APPLIED IN THIS STEP
-- That is, the product of QC should enrich final results as an onPanel flag
-- with domain {0,1}, where 1 means a Shop is on Panel.
-- Also, consider adding QC for the Universe, as it will decrease the final
-- number of Shops of the entire STEP#2
-- !!!!!!!!
-- Also, DOĞU ANADOLU and GÜNEYDOĞU ANADOLU have been combined as per design
-- fulfilment into ‘DOĞU/GÜNEYDOĞU ANADOLU’ in this step.
select
    imscode,
    case Bolge
      when 'DOĞU ANADOLU' then 'DOĞU/GÜNEYDOĞU ANADOLU'
      when 'GÜNEYDOĞU ANADOLU' then 'DOĞU/GÜNEYDOĞU ANADOLU'
      else Bolge
    end as Region,
    1 as onUnvrs, -- this needs to come from production QC
    1 as onPanel  -- this needs to come from production QC
  into dbo.tmpPjf_021_ShopsBase
  from [10.165.64.30].[IMSDAS_PROJECT_TP].dbo.nt70das1 nt
  left join [10.165.64.30].[IMSDAS_PROJECT_TP].dbo.Region r on r.brick1 = nt.Brick
  left join [DWPTREND].DBO.TBOLGELER TB ON Brickcode = tcode1001
;

-- drop table dbo.tmpPjf_022_ShopsWx;
--
-- Elapsed times
-- 1 mth projected: 25 mins (VERY LONG!)

-- Sell-In volume to build {'small', 'medium', 'large'} TSC baskets
select CustomerCode, sum(TotalUnit) Units
  into dbo.tmpPjf_022_ShopsWx
  from IMSDAS_PROJECT_TURKEY_PHARMATREND_MONTHLY.dbo.BackSellInData
 where 1=1
    -- we want only 'known' Packs
   and exists (select 1 from IMSDAS_PROJECT_TURKEY_PHARMATREND.dbo.Packtemp where substring(BRIDGE_CODE_2,2,7) = PFC)
    -- we want only Shops that are onUnvrs
   and exists (select 1 from dbo.tmpPjf_021_ShopsBase where onUnvrs=1 and imscode = CustomerCode)
    -- last full sell-in year
   and Period between
    -- thank you, MS$, for your SQL dialect...
    ( select cast (substring(convert(varchar, dateadd(year, -1, convert(datetime,substring(cast(max(PdID) as varchar),1,4)+'-01-01',120)),112),1,6) as int) from tmpPjfPdOper )
    and
    ( select cast (substring(convert(varchar, dateadd(month,11,dateadd(year, -1, convert(datetime,substring(cast(max(PdID) as varchar),1,4)+'-01-01',120))),112),1,6) as int) from tmpPjfPdOper )
 group by CustomerCode
;

-- drop table dbo.tmpPjf_023_ShopsWxOrphan;

-- 'Orphan' Shops that are not in TSC range (e.g. full 2016), but are in the
-- Potential range (e.g. previous 9 months to currently projected month + the
-- currently projected month). They will be assign region avarage Units.
select CustomerCode
  into dbo.tmpPjf_023_ShopsWxOrphan
  from IMSDAS_PROJECT_TURKEY_PHARMATREND_MONTHLY.dbo.BackSellInData Wx
  join ( select distinct PdID from dbo.tmpPjf_010_Pd union all select distinct ProcPdID from dbo.tmpPjf_010_Pd ) Pd
    on Pd.PdID = Period
 where 1=1
    -- only 'orphans'
   and not exists (select 1 from dbo.tmpPjf_022_ShopsWx Unvrs where Unvrs.CustomerCode = Wx.CustomerCode)
    -- we want only 'known' Packs
   and exists (select 1 from IMSDAS_PROJECT_TURKEY_PHARMATREND.dbo.Packtemp where substring(BRIDGE_CODE_2,2,7) = PFC)
    -- we want only Shops that are onUnvrs
   and exists (select 1 from dbo.tmpPjf_021_ShopsBase where onUnvrs=1 and imscode = CustomerCode)
 group by CustomerCode
;

-- drop table dbo.tmpPjf_024_ShopsUnvrs;

-- Build the Universe with all Shops affected, Shop-to-Region assigment
select cast (CustomerCode as int) as ShopID, Region, onPanel, Units
  into dbo.tmpPjf_024_ShopsUnvrs
  from (select CustomerCode, Units from dbo.tmpPjf_022_ShopsWx union all select CustomerCode, null from dbo.tmpPjf_023_ShopsWxOrphan) Unvrs
  join dbo.tmpPjf_021_ShopsBase
    on imscode = CustomerCode
;

-- drop table dbo.tmpPjf_025_ShopsSorted;

-- Median calculation for 'orphan' Shops and ordering for cumulative sum
--
-- PERCENTILE_CONT function is not allowed in the current compatibility mode.
-- Really, it took you up to version 2012 to provide Median calculation, MS$?
with
Sorted as (
  select Region, ShopID, Units, count(ShopID) over (partition by Region) cnt, row_number() over (partition by Region order by Units) rn
    from dbo.tmpPjf_024_ShopsUnvrs
   where Units is not null -- only TSC shops should count for Region Median
),
Median as (
  select Region, avg(Units) as UnitsAvg from Sorted where rn between (cnt + 1)/2 and (cnt + 2)/2 group by Region
)
select Unvrs.Region, ShopID, onPanel, isnull(Units, UnitsAvg) as Units, row_number() over (partition by null order by Units desc) as RN
  into dbo.tmpPjf_025_ShopsSorted
  from dbo.tmpPjf_024_ShopsUnvrs Unvrs
  join Median m
    on Unvrs.Region = m.Region

-- drop table dbo.tmpPjf_026_ShopsCUM;

-- Compute cumulative SUM for each Shop ordered by descending Units
select t1.ShopID, t1.Region, t1.onPanel, t1.Units, sum(cast (t2.Units as bigint)) as UnitsCUM -- need casting, overflow for int
  into dbo.tmpPjf_026_ShopsCUM
  from dbo.tmpPjf_025_ShopsSorted t1
  inner join tmpPjf_025_ShopsSorted t2 on t1.rn >= t2.rn
  group by t1.Region, t1.ShopID, t1.onPanel, t1.Units
;

-- drop table dbo.tmpPjf_027_Shops;

-- Assign TSC class based on UnitsCUM and promote into dbo.tmpPjf_020_Shops
-- 20% of total turnover - small pharmacies
-- 30% of total turnover - medium pharmacies
-- 50% of total turnover - large pharmacies
with
Bounds as (
  select min(UnitsCUM) as minUnitsCUM, max(UnitsCUM) as maxUnitsCUM from tmpPjf_026_ShopsCUM
)
select
ShopID,
Region,
case
  when cast (UnitsCUM as float)/maxUnitsCUM < 1.0/2 then 3 -- 'large' 50%
  when cast (UnitsCUM as float)/maxUnitsCUM > 4.0/5 then 1 -- 'small' 20%
  else 2 -- 'medium' 30%
end as TSC,
1 as Chain,
onPanel  -- THIS NEEDS TO BE A PRODUCT OF QC, IT IS USED IN STEP#5 FOR COMPUTING PANEL POTENTIAL
into dbo.tmpPjf_027_Shops
from dbo.tmpPjf_026_ShopsCUM cross join Bounds
;

-- Validation query:
--
-- select
--     Region,
--     count(distinct case when TSC = 1 then ShopID else null end) as Stores_TSC1,
--     count(distinct case when TSC = 2 then ShopID else null end) as Stores_TSC2,
--     count(distinct case when TSC = 3 then ShopID else null end) as Stores_TSC3,
--     count(distinct ShopID) Stores_TOT
--   from tmpPjf_027_Shops
--  group by Region
--  order by 1;


--------------------------------------------------------------------------------
-- STEP#3 Packs
-- Note:
-- Collect product references for all packs in the Universe. This step needs
-- MAJOR revision. First, we need data from the Stats Office imported into
-- dbo.tmpPjfProdFlag, otherwise default Product Flag is used. I assumed the
-- default for the ProdFlag is Basket level. Then, for Universe or Panel Sell-In
-- and Sell-Out only Packs for which basket has been provided by the Stats
-- Office are considered for computation (inner join).
--
-- !!! Only baskets defined by SO are considered currently !!!

-- drop table dbo.tmpPjf_030_Packs;

select
    cast (BRIDGE_CODE_2 as int) as PackID,
    PROD_DESC as Brand, -- is this a Brand code?
    substring(CLASS_CODE_1, 1,1) as ATC1,
    substring(CLASS_CODE_1, 1,3) as ATC2,
    substring(CLASS_CODE_1, 1,4) as ATC3,
    CLASS_CODE_1 as ATC4,
    PFG as Basket, -- should there be any default Basket?
    case when PACK_FLAG_1 = 'PRESC' then '1' else '0' end as Rx,
    case lower(isnull(ProdFlag, 'basket'))
      when 'pfc' then cast (cast (BRIDGE_CODE_2 as int) as varchar)
      when 'brand' then PROD_DESC
      when 'atc1' then substring(CLASS_CODE_1,1,1)
      when 'atc2' then substring(CLASS_CODE_1,1,3)
      when 'atc3' then substring(CLASS_CODE_1,1,4)
      when 'atc4' then CLASS_CODE_1
      when 'basket' then PFG
      when 'rx' then case when PACK_FLAG_1 = 'PRESC' then '1' else '0' end
      else PFG -- default projection class is 'basket'
    end as Class
  into dbo.tmpPjf_030_Packs
  from IMSDAS_PROJECT_TURKEY_PHARMATREND.dbo.Packtemp p
  -- Consider outer join, and provide a default 'basket' if missing form the Stats Office
  join dbo.tmpPjfPFG pfg
    on p.BRIDGE_CODE_2 = RIGHT(REPLICATE('0', 8) + LEFT(pfg.pfc, 8), 8)
  left outer join dbo.tmpPjfProdFlag pf
    on p.BRIDGE_CODE_2 = RIGHT(REPLICATE('0', 8) + LEFT(pf.pfc, 8), 8)
;


--------------------------------------------------------------------------------
-- STEP#4 Universe
-- Note:
-- Build the Sell-In universe. Quality checks to be added, or QC implemented
-- somewhere upstream.
--
-- Sell-in base is prepared here. Data for all the projected months is collected
-- from the Sell-In and aggregated. For example, if currently processed months
-- are 201711 and 201707 then 201610..201707..201710 are collected and aggregated
-- for each projected month, taking into account Sell-In should be one months
-- in arrears to Sell-Out, and that MthsAvg to build potential is 9 months.
--
-- This step takes some time time to execute (e.g. >10 mins).

-- drop table dbo.tmpPjf_040_Unvrs;
--
-- Elapsed times
-- 1 mth projected: 1.5 mins

select ProcPdID as PdID, ShopID, PackID, sum(TotalUnit) Units
  into dbo.tmpPjf_040_Unvrs
  from IMSDAS_PROJECT_TURKEY_PHARMATREND_MONTHLY.dbo.BackSellInData
  join dbo.tmpPjf_010_Pd
    on PdID = Period
  join dbo.tmpPjf_027_Shops
    on RIGHT(REPLICATE('0', 7) + LEFT(ShopID, 7), 7) = CustomerCode
  join dbo.tmpPjf_030_Packs
    on RIGHT(REPLICATE('0', 7) + LEFT(PackID, 7), 7) = PFC
 group by ProcPdID, ShopID, PackID
;

--------------------------------------------------------------------------------
-- STEP#5 Panel
-- Note:
-- Build the Sell-Out panel. Quality checks to be added, or QC implemented
-- somewhere upstream. Current assumption is that every shop that has any
-- dispensation data for currently projected month will be onPanel.

-- drop table dbo.tmpPjf_050_Panel;

select ProcPdID as PdID, ShopID, PackID
  into dbo.tmpPjf_050_Panel
  from IMSDAS_PROJECT_TURKEY_PHARMATREND_MONTHLY.dbo.TSMBackData tx
  join (select distinct ProcPdID from dbo.tmpPjf_010_Pd) pd
  -- please, please do something with that "Date" attribute on TSMBackData...
    on ProcPdID = concat(substring(convert(varchar, cast ("Date" as date), 112),1,4),substring(convert(varchar, cast ("Date" as date), 112),7,2))
  join dbo.tmpPjf_027_Shops
    on RIGHT(REPLICATE('0', 7) + LEFT(ShopID, 7), 7) = IMSCustomerCode
  join dbo.tmpPjf_030_Packs
    on RIGHT(REPLICATE('0', 7) + LEFT(PackID, 7), 7) = PFC
  -- only Sales & Returns, no Stock
 where tx.RecordType<>'03'
  -- pay attention to onPanel flag; this should be a product of QC provided in STEP#2.1
   and onPanel=1
 group by ProcPdID, ShopID, PackID
;


--------------------------------------------------------------------------------
-- STEP#6 Universe Sell-In
-- Note:
-- Compute Universe Sell-In MthsAvg for each month {Period x Shop x Pack}.

-- drop table dbo.tmpPjf_060_UnvrsWxBase;
--
-- Elapsed times
-- 1 mth projected: 4 mins

select
    PdID, ux.ShopID, ux.PackID, Region, TSC, Chain, Brand, ATC1, ATC2, ATC3, ATC4, Basket, Rx, Class, Units
  into dbo.tmpPjf_060_UnvrsWxBase
  from dbo.tmpPjf_040_Unvrs ux
  join dbo.tmpPjf_027_Shops s
    on ux.ShopID = s.ShopID
  join dbo.tmpPjf_030_Packs p
    on ux.PackID = p.PackID
;

-- drop table dbo.tmpPjf_061_UnvrsWxClass;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, Class, sum(Units)/MthsAvg as UnvrsWxClassUnits
  into dbo.tmpPjf_061_UnvrsWxClass
  from dbo.tmpPjf_060_UnvrsWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, Class, MthsAvg
;

-- drop table dbo.tmpPjf_062_UnvrsWxBrand;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, Brand, sum(Units)/MthsAvg as UnvrsWxBrandUnits
  into dbo.tmpPjf_062_UnvrsWxBrand
  from dbo.tmpPjf_060_UnvrsWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, Brand, MthsAvg
;

-- drop table dbo.tmpPjf_063_UnvrsWxATC4;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, ATC4, sum(Units)/MthsAvg as UnvrsWxATC4Units
  into dbo.tmpPjf_063_UnvrsWxATC4
  from dbo.tmpPjf_060_UnvrsWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, ATC4, MthsAvg
;

-- drop table dbo.tmpPjf_064_UnvrsWxATC3;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, ATC3, sum(Units)/MthsAvg as UnvrsWxATC3Units
  into dbo.tmpPjf_064_UnvrsWxATC3
  from dbo.tmpPjf_060_UnvrsWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, ATC3, MthsAvg
;

-- drop table dbo.tmpPjf_065_UnvrsWxATC2;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, ATC2, sum(Units)/MthsAvg as UnvrsWxATC2Units
  into dbo.tmpPjf_065_UnvrsWxATC2
  from dbo.tmpPjf_060_UnvrsWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, ATC2, MthsAvg
;

-- drop table dbo.tmpPjf_066_UnvrsWxATC1;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, ATC1, sum(Units)/MthsAvg as UnvrsWxATC1Units
  into dbo.tmpPjf_066_UnvrsWxATC1
  from dbo.tmpPjf_060_UnvrsWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, ATC1, MthsAvg
;

-- drop table dbo.tmpPjf_067_UnvrsWxBasket;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, Basket, sum(Units)/MthsAvg as UnvrsWxBasketUnits
  into dbo.tmpPjf_067_UnvrsWxBasket
  from dbo.tmpPjf_060_UnvrsWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, Basket, MthsAvg
;

-- drop table dbo.tmpPjf_068_UnvrsWxRx;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, Rx, sum(Units)/MthsAvg as UnvrsWxRxUnits
  into dbo.tmpPjf_068_UnvrsWxRx
  from dbo.tmpPjf_060_UnvrsWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, Rx, MthsAvg
;

-- drop table dbo.tmpPjf_069_UnvrsWx;
--
-- Elapsed times
-- 1 mth projected: 7 mins

select
    t60.PdID, t60.ShopID, t60.PackID, t60.Region, t60.TSC, t60.Chain, t60.Brand, t60.ATC4, t60.ATC3, t60.ATC2, t60.ATC1, t60.Basket, t60.Rx, t60.Class,
    case
      when t60.Class = t60.Brand then UnvrsWxBrandUnits
      when t60.Class = t60.ATC4 then UnvrsWxATC4Units
      when t60.Class = t60.ATC3 then UnvrsWxATC3Units
      when t60.Class = t60.ATC2 then UnvrsWxATC2Units
      when t60.Class = t60.ATC1 then UnvrsWxATC1Units
      when t60.Class = t60.Basket then UnvrsWxBasketUnits
      when t60.Class = t60.Rx then UnvrsWxRxUnits
      else UnvrsWxClassUnits
    end as UnvrsWxClassUnits,
    UnvrsWxBrandUnits,
    UnvrsWxATC4Units,
    UnvrsWxATC3Units,
    UnvrsWxATC2Units,
    UnvrsWxATC1Units,
    UnvrsWxBasketUnits,
    UnvrsWxRxUnits
  into dbo.tmpPjf_069_UnvrsWx
  from dbo.tmpPjf_060_UnvrsWxBase t60
  left join dbo.tmpPjf_061_UnvrsWxClass t61
    on t60.PdID = t61.PdID and t60.Region = t61.Region and t60.TSC = t61.TSC and t60.Chain = t61.Chain and t60.Brand = t61.Class
  left join dbo.tmpPjf_062_UnvrsWxBrand t62
    on t60.PdID = t62.PdID and t60.Region = t62.Region and t60.TSC = t62.TSC and t60.Chain = t62.Chain and t60.Brand = t62.Brand
  left join dbo.tmpPjf_063_UnvrsWxATC4 t63
    on t60.PdID = t63.PdID and t60.Region = t63.Region and t60.TSC = t63.TSC and t60.Chain = t63.Chain and t60.ATC4 = t63.ATC4
  left join dbo.tmpPjf_064_UnvrsWxATC3 t64
    on t60.PdID = t64.PdID and t60.Region = t64.Region and t60.TSC = t64.TSC and t60.Chain = t64.Chain and t60.ATC3 = t64.ATC3
  left join dbo.tmpPjf_065_UnvrsWxATC2 t65
    on t60.PdID = t65.PdID and t60.Region = t65.Region and t60.TSC = t65.TSC and t60.Chain = t65.Chain and t60.ATC2 = t65.ATC2
  left join dbo.tmpPjf_066_UnvrsWxATC1 t66
    on t60.PdID = t66.PdID and t60.Region = t66.Region and t60.TSC = t66.TSC and t60.Chain = t66.Chain and t60.ATC1 = t66.ATC1
  left join dbo.tmpPjf_067_UnvrsWxBasket t67
    on t60.PdID = t67.PdID and t60.Region = t67.Region and t60.TSC = t67.TSC and t60.Chain = t67.Chain and t60.Basket = t67.Basket
  left join dbo.tmpPjf_068_UnvrsWxRx t68
    on t60.PdID = t68.PdID and t60.Region = t68.Region and t60.TSC = t68.TSC and t60.Chain = t68.Chain and t60.Rx = t68.Rx
;

--------------------------------------------------------------------------------
-- STEP#7 Panel Sell-In
-- Note:
-- Compute Panel Sell-In MthsAvg for each month {Period x Shop x Pack}.

-- drop table dbo.tmpPjf_070_PanelWxBase;

with XPanel as (
select
    PdID, px.ShopID, px.PackID, Region, TSC, Chain, Brand, ATC1, ATC2, ATC3, ATC4, Basket, Rx, Class
  from dbo.tmpPjf_050_Panel px
  join dbo.tmpPjf_027_Shops s
    on px.ShopID = s.ShopID
  join dbo.tmpPjf_030_Packs p
    on px.PackID = p.PackID
)
select
    PdID, ux.ShopID, ux.PackID, Region, TSC, Chain, Brand, ATC1, ATC2, ATC3, ATC4, Basket, Rx, Class, Units
  into dbo.tmpPjf_070_PanelWxBase
  from dbo.tmpPjf_040_Unvrs ux
  join dbo.tmpPjf_027_Shops s
    on ux.ShopID = s.ShopID
  join dbo.tmpPjf_030_Packs p
    on ux.PackID = p.PackID
 where exists
  (select 1 from XPanel px where ux.PdID = px.PdID and ux.ShopID = px.ShopID)
;

-- drop table dbo.tmpPjf_071_PanelWxClass;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select PdID, Region, TSC, Chain, Class, sum(Units)/MthsAvg as PanelWxClassUnits
  into dbo.tmpPjf_071_PanelWxClass
  from dbo.tmpPjf_070_PanelWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, Class, MthsAvg
;

-- drop table dbo.tmpPjf_072_PanelWxBrand;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select PdID, Region, TSC, Chain, Brand, sum(Units)/MthsAvg as PanelWxBrandUnits
  into dbo.tmpPjf_072_PanelWxBrand
  from dbo.tmpPjf_070_PanelWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, Brand, MthsAvg
;

-- drop table dbo.tmpPjf_073_PanelWxATC4;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, ATC4, sum(Units)/MthsAvg as PanelWxATC4Units
  into dbo.tmpPjf_073_PanelWxATC4
  from dbo.tmpPjf_070_PanelWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, ATC4, MthsAvg
;

-- drop table dbo.tmpPjf_074_PanelWxATC3;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, ATC3, sum(Units)/MthsAvg as PanelWxATC3Units
  into dbo.tmpPjf_074_PanelWxATC3
  from dbo.tmpPjf_070_PanelWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, ATC3, MthsAvg
;

-- drop table dbo.tmpPjf_075_PanelWxATC2;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, ATC2, sum(Units)/MthsAvg as PanelWxATC2Units
  into dbo.tmpPjf_075_PanelWxATC2
  from dbo.tmpPjf_070_PanelWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, ATC2, MthsAvg
;

-- drop table dbo.tmpPjf_076_PanelWxATC1;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, ATC1, sum(Units)/MthsAvg as PanelWxATC1Units
  into dbo.tmpPjf_076_PanelWxATC1
  from dbo.tmpPjf_070_PanelWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, ATC1, MthsAvg
;

-- drop table dbo.tmpPjf_077_PanelWxBasket;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, Basket, sum(Units)/MthsAvg as PanelWxBasketUnits
  into dbo.tmpPjf_077_PanelWxBasket
  from dbo.tmpPjf_070_PanelWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, Basket, MthsAvg
;

-- drop table dbo.tmpPjf_078_PanelWxRx;

with Ctrl as ( select cast (val as int) as MthsAvg from dbo.tmpPjfCtrl where param = 'MthsAvg' )
select
    PdID, Region, TSC, Chain, Rx, sum(Units)/MthsAvg PanelWxRxUnits
  into dbo.tmpPjf_078_PanelWxRx
  from dbo.tmpPjf_070_PanelWxBase cross join Ctrl
 group by PdID, Region, TSC, Chain, Rx, MthsAvg
;


-- drop table dbo.tmpPjf_079_PanelWx;

select
    t70.PdID, t70.ShopID, t70.PackID, t70.Region, t70.TSC, t70.Chain, t70.Brand, t70.ATC4, t70.ATC3, t70.ATC2, t70.ATC1, t70.Basket, t70.Rx, t70.Class,
    case
      when t70.Class = t70.Brand then PanelWxBrandUnits
      when t70.Class = t70.ATC4 then PanelWxATC4Units
      when t70.Class = t70.ATC3 then PanelWxATC3Units
      when t70.Class = t70.ATC2 then PanelWxATC2Units
      when t70.Class = t70.ATC1 then PanelWxATC1Units
      when t70.Class = t70.Basket then PanelWxBasketUnits
      when t70.Class = t70.Rx then PanelWxRxUnits
      else PanelWxClassUnits
    end as PanelWxClassUnits,
    PanelWxBrandUnits,
    PanelWxATC4Units,
    PanelWxATC3Units,
    PanelWxATC2Units,
    PanelWxATC1Units,
    PanelWxBasketUnits,
    PanelWxRxUnits
  into dbo.tmpPjf_079_PanelWx
  from dbo.tmpPjf_050_Panel px
  join dbo.tmpPjf_070_PanelWxBase t70
    on t70.PdID = px.PdID and t70.ShopID = px.ShopID and t70.PackID = px.PackID
  left join dbo.tmpPjf_071_PanelWxClass t71
    on t70.PdID = t71.PdID and t70.Region = t71.Region and t70.TSC = t71.TSC and t70.Chain = t71.Chain and t70.Brand = t71.Class
  left join dbo.tmpPjf_072_PanelWxBrand t72
    on t70.PdID = t72.PdID and t70.Region = t72.Region and t70.TSC = t72.TSC and t70.Chain = t72.Chain and t70.Brand = t72.Brand
  left join dbo.tmpPjf_073_PanelWxATC4 t73
    on t70.PdID = t73.PdID and t70.Region = t73.Region and t70.TSC = t73.TSC and t70.Chain = t73.Chain and t70.ATC4 = t73.ATC4
  left join dbo.tmpPjf_074_PanelWxATC3 t74
    on t70.PdID = t74.PdID and t70.Region = t74.Region and t70.TSC = t74.TSC and t70.Chain = t74.Chain and t70.ATC3 = t74.ATC3
  left join dbo.tmpPjf_075_PanelWxATC2 t75
    on t70.PdID = t75.PdID and t70.Region = t75.Region and t70.TSC = t75.TSC and t70.Chain = t75.Chain and t70.ATC2 = t75.ATC2
  left join dbo.tmpPjf_076_PanelWxATC1 t76
    on t70.PdID = t76.PdID and t70.Region = t76.Region and t70.TSC = t76.TSC and t70.Chain = t76.Chain and t70.ATC1 = t76.ATC1
  left join dbo.tmpPjf_077_PanelWxBasket t77
    on t70.PdID = t77.PdID and t70.Region = t77.Region and t70.TSC = t77.TSC and t70.Chain = t77.Chain and t70.Basket = t77.Basket
  left join dbo.tmpPjf_078_PanelWxRx t78
    on t70.PdID = t78.PdID and t70.Region = t78.Region and t70.TSC = t78.TSC and t70.Chain = t78.Chain and t70.Rx = t78.Rx
;

--------------------------------------------------------------------------------
-- STEP#8 Universe Potential
-- Note:

-- drop table dbo.tmpPjf_080_Potential;
--
-- Elapsed times
-- 1 mth projected: 10 mins

select distinct
    PdID, Region, TSC, Chain, Brand, ATC4, ATC3, ATC2, ATC1, Basket, Rx, Class,
    UnvrsWxClassUnits, UnvrsWxBrandUnits, UnvrsWxATC4Units, UnvrsWxATC3Units, UnvrsWxATC2Units, UnvrsWxATC1Units, UnvrsWxBasketUnits, UnvrsWxRxUnits
  into dbo.tmpPjf_080_Potential
  from dbo.tmpPjf_069_UnvrsWx
;

--------------------------------------------------------------------------------
-- STEP#9 Panel Orphans
-- Note:
-- Handle Packs that have been dispensed, but there's no Sell-In data available.
-- In such case, project at Brand level, then try ATC4, then AT3, etc.
-- (whichever is available first).
-- Due to some issues with performance multiple LEFT OUTER joins were split into
-- several steps (could be as well written within a single query...).

-- drop table dbo.tmpPjf_090_Orphans;

with XPanel as (
select
    PdID, px.ShopID, px.PackID, Region, TSC, Chain, Brand, ATC1, ATC2, ATC3, ATC4, Basket, Rx, Class
  from dbo.tmpPjf_050_Panel px
  join dbo.tmpPjf_027_Shops s
    on px.ShopID = s.ShopID
  join dbo.tmpPjf_030_Packs p
    on px.PackID = p.PackID
)
select
    PdID, px.ShopID, px.PackID, Region, TSC, Chain, Brand, ATC1, ATC2, ATC3, ATC4, Basket, Rx, Class
  into dbo.tmpPjf_090_Orphans
  from XPanel px
where not exists (select 1 from dbo.tmpPjf_060_UnvrsWxBase wx where px.PdID = wx.PdID and px.ShopID = wx.ShopID and px.PackID = wx.PackID)
;

-- drop table dbo.tmpPjf_091_OrphansWxClass;

select
    ox.PdID, ox.ShopID, ox.PackID, ox.Region, ox.TSC, ox.Chain, ox.Brand, ox.ATC1, ox.ATC2, ox.ATC3, ox.ATC4, ox.Basket, ox.Rx, ox.Class, UnvrsWxClassUnits, PanelWxClassUnits
  into dbo.tmpPjf_091_OrphansWxClass
  from dbo.tmpPjf_090_Orphans ox
  left join ( select distinct PdID, Region, TSC, Chain, Class, PanelWxClassUnits from dbo.tmpPjf_079_PanelWx where PanelWxClassUnits is not null) pwxClass
    on ox.PdID = pwxClass.PdID
    and ox.Region = pwxClass.Region
    and ox.TSC = pwxClass.TSC
    and ox.Chain = pwxClass.Chain
    and ox.Class = pwxClass.Class
  left join ( select distinct PdID, Region, TSC, Chain, Class, UnvrsWxClassUnits from dbo.tmpPjf_080_Potential ) potClass
    on ox.PdID = potClass.PdID
    and ox.Region = potClass.Region
    and ox.TSC = potClass.TSC
    and ox.Chain = potClass.Chain
    and ox.Class = potClass.Class
;

-- drop table dbo.tmpPjf_092_OrphansWxBrand;

select
    ox.*, UnvrsWxBrandUnits, PanelWxBrandUnits
  into dbo.tmpPjf_092_OrphansWxBrand
  from dbo.tmpPjf_091_OrphansWxClass ox
  left join ( select distinct PdID, Region, TSC, Chain, Brand, PanelWxBrandUnits from dbo.tmpPjf_079_PanelWx where PanelWxBrandUnits is not null) pwxBrand
    on ox.PdID = pwxBrand.PdID
    and ox.Region = pwxBrand.Region
    and ox.TSC = pwxBrand.TSC
    and ox.Chain = pwxBrand.Chain
    and ox.Brand = pwxBrand.Brand
  left join ( select distinct PdID, Region, TSC, Chain, Brand, UnvrsWxBrandUnits from dbo.tmpPjf_080_Potential ) potBrand
    on ox.PdID = potBrand.PdID
    and ox.Region = potBrand.Region
    and ox.TSC = potBrand.TSC
    and ox.Chain = potBrand.Chain
    and ox.Brand = potBrand.Brand
;

-- drop table dbo.tmpPjf_093_OrphansWxATC4;

select
    ox.*, UnvrsWxATC4Units, PanelWxATC4Units
  into dbo.tmpPjf_093_OrphansWxATC4
  from dbo.tmpPjf_092_OrphansWxBrand ox
  left join ( select distinct PdID, Region, TSC, Chain, ATC4, PanelWxATC4Units from dbo.tmpPjf_079_PanelWx where PanelWxATC4Units is not null) pwxATC4
    on ox.PdID = pwxATC4.PdID
    and ox.Region = pwxATC4.Region
    and ox.TSC = pwxATC4.TSC
    and ox.Chain = pwxATC4.Chain
    and ox.ATC4 = pwxATC4.ATC4
  left join ( select distinct PdID, Region, TSC, Chain, ATC4, UnvrsWxATC4Units from dbo.tmpPjf_080_Potential ) potATC4
    on ox.PdID = potATC4.PdID
    and ox.Region = potATC4.Region
    and ox.TSC = potATC4.TSC
    and ox.Chain = potATC4.Chain
    and ox.ATC4 = potATC4.ATC4
;

-- drop table dbo.tmpPjf_094_OrphansWxATC3;

select
    ox.*, UnvrsWxATC3Units, PanelWxATC3Units
  into dbo.tmpPjf_094_OrphansWxATC3
  from dbo.tmpPjf_093_OrphansWxATC4 ox
  left join ( select distinct PdID, Region, TSC, Chain, ATC3, PanelWxATC3Units from dbo.tmpPjf_079_PanelWx where PanelWxATC3Units is not null) pwxATC3
    on ox.PdID = pwxATC3.PdID
    and ox.Region = pwxATC3.Region
    and ox.TSC = pwxATC3.TSC
    and ox.Chain = pwxATC3.Chain
    and ox.ATC3 = pwxATC3.ATC3
  left join ( select distinct PdID, Region, TSC, Chain, ATC3, UnvrsWxATC3Units from dbo.tmpPjf_080_Potential ) potATC3
    on ox.PdID = potATC3.PdID
    and ox.Region = potATC3.Region
    and ox.TSC = potATC3.TSC
    and ox.Chain = potATC3.Chain
    and ox.ATC3 = potATC3.ATC3
;

-- drop table dbo.tmpPjf_095_OrphansWxATC2;

select
    ox.*, UnvrsWxATC2Units, PanelWxATC2Units
  into dbo.tmpPjf_095_OrphansWxATC2
  from dbo.tmpPjf_094_OrphansWxATC3 ox
  left join ( select distinct PdID, Region, TSC, Chain, ATC2, PanelWxATC2Units from dbo.tmpPjf_079_PanelWx where PanelWxATC2Units is not null) pwxATC2
    on ox.PdID = pwxATC2.PdID
    and ox.Region = pwxATC2.Region
    and ox.TSC = pwxATC2.TSC
    and ox.Chain = pwxATC2.Chain
    and ox.ATC2 = pwxATC2.ATC2
  left join ( select distinct PdID, Region, TSC, Chain, ATC2, UnvrsWxATC2Units from dbo.tmpPjf_080_Potential ) potATC2
    on ox.PdID = potATC2.PdID
    and ox.Region = potATC2.Region
    and ox.TSC = potATC2.TSC
    and ox.Chain = potATC2.Chain
    and ox.ATC2 = potATC2.ATC2
;

-- drop table dbo.tmpPjf_096_OrphansWxATC1;

select
    ox.*, UnvrsWxATC1Units, PanelWxATC1Units
  into dbo.tmpPjf_096_OrphansWxATC1
  from dbo.tmpPjf_095_OrphansWxATC2 ox
  left join ( select distinct PdID, Region, TSC, Chain, ATC1, PanelWxATC1Units from dbo.tmpPjf_079_PanelWx where PanelWxATC1Units is not null) pwxATC1
    on ox.PdID = pwxATC1.PdID
    and ox.Region = pwxATC1.Region
    and ox.TSC = pwxATC1.TSC
    and ox.Chain = pwxATC1.Chain
    and ox.ATC1 = pwxATC1.ATC1
  left join ( select distinct PdID, Region, TSC, Chain, ATC1, UnvrsWxATC1Units from dbo.tmpPjf_080_Potential ) potATC1
    on ox.PdID = potATC1.PdID
    and ox.Region = potATC1.Region
    and ox.TSC = potATC1.TSC
    and ox.Chain = potATC1.Chain
    and ox.ATC1 = potATC1.ATC1
;

-- drop table dbo.tmpPjf_097_OrphansWxBasket;

select
    ox.*, UnvrsWxBasketUnits, PanelWxBasketUnits
  into dbo.tmpPjf_097_OrphansWxBasket
  from dbo.tmpPjf_096_OrphansWxATC1 ox
  left join ( select distinct PdID, Region, TSC, Chain, Basket, PanelWxBasketUnits from dbo.tmpPjf_079_PanelWx where PanelWxBasketUnits is not null) pwxBasket
    on ox.PdID = pwxBasket.PdID
    and ox.Region = pwxBasket.Region
    and ox.TSC = pwxBasket.TSC
    and ox.Chain = pwxBasket.Chain
    and ox.Basket = pwxBasket.Basket
  left join ( select distinct PdID, Region, TSC, Chain, Basket, UnvrsWxBasketUnits from dbo.tmpPjf_080_Potential ) potBasket
    on ox.PdID = potBasket.PdID
    and ox.Region = potBasket.Region
    and ox.TSC = potBasket.TSC
    and ox.Chain = potBasket.Chain
    and ox.Basket = potBasket.Basket
;

-- drop table dbo.tmpPjf_098_OrphansWxRx;

select
    ox.*, UnvrsWxRxUnits, PanelWxRxUnits
  into dbo.tmpPjf_098_OrphansWxRx
  from dbo.tmpPjf_097_OrphansWxBasket ox
  left join ( select distinct PdID, Region, TSC, Chain, Rx, PanelWxRxUnits from dbo.tmpPjf_079_PanelWx where PanelWxRxUnits is not null) pwxRx
    on ox.PdID = pwxRx.PdID
    and ox.Region = pwxRx.Region
    and ox.TSC = pwxRx.TSC
    and ox.Chain = pwxRx.Chain
    and ox.Rx = pwxRx.Rx
  left join ( select distinct PdID, Region, TSC, Chain, Rx, UnvrsWxRxUnits from dbo.tmpPjf_080_Potential ) potRx
    on ox.PdID = potRx.PdID
    and ox.Region = potRx.Region
    and ox.TSC = potRx.TSC
    and ox.Chain = potRx.Chain
    and ox.Rx = potRx.Rx
;

-- drop table dbo.tmpPjf_099_OrphansWx;

select *
  into dbo.tmpPjf_099_OrphansWx -- REMOVE, COSMETIC...
  from dbo.tmpPjf_098_OrphansWxRx
;

--------------------------------------------------------------------------------
-- STEP#10 PF
-- Note:
-- Finish.

-- drop table dbo.tmpPjf_100_Pjf;

select distinct
    pwxClass.PdID,
    pwxClass.ShopID,
    pwxClass.PackID,
    case
      when isnull(PanelWxClassUnits,0)>0 then cast (UnvrsWxClassUnits as float)/PanelWxClassUnits
      else null
    end as PF,
    pwxClass.Region,
    pwxClass.TSC,
    pwxClass.Chain,
    pwxClass.Class,
    0 as Orphan
  into dbo.tmpPjf_100_Pjf
  from
  ( select PdID, ShopID, PackID, Region, TSC, Chain, Class, PanelWxClassUnits from dbo.tmpPjf_079_PanelWx ) pwxClass
 inner join ( select distinct PdID, Region, TSC, Chain, Class, UnvrsWxClassUnits from dbo.tmpPjf_080_Potential ) potClass
    on pwxClass.PdID = potClass.PdID
   and pwxClass.Region = potClass.Region
   and pwxClass.TSC = potClass.TSC
   and pwxClass.Chain = potClass.Chain
   and pwxClass.Class = potClass.Class
 union all
select
    PdID,
    ShopID,
    PackID,
    -- if Panel Sell-In not available in projected class then try Brand->ATC4->ATC3->etc. until it's there
    case
      when isnull(PanelWxClassUnits,0)>0 then cast (UnvrsWxClassUnits as float)/PanelWxClassUnits
      when isnull(PanelWxBrandUnits,0)>0 then cast (UnvrsWxBrandUnits as float)/PanelWxBrandUnits
      when isnull(PanelWxATC4Units,0)>0 then cast (UnvrsWxATC4Units as float)/PanelWxATC4Units
      when isnull(PanelWxATC3Units,0)>0 then cast (UnvrsWxATC3Units as float)/PanelWxATC3Units
      when isnull(PanelWxATC2Units,0)>0 then cast (UnvrsWxATC2Units as float)/PanelWxATC2Units
      when isnull(PanelWxATC1Units,0)>0 then cast (UnvrsWxATC1Units as float)/PanelWxATC1Units
      when isnull(PanelWxBasketUnits,0)>0 then cast (UnvrsWxBasketUnits as float)/PanelWxBasketUnits
      when isnull(PanelWxRxUnits,0)>0 then cast (UnvrsWxRxUnits as float)/PanelWxRxUnits
      else null
    end as PF,
    Region,
    TSC,
    Chain,
    case
      when isnull(PanelWxClassUnits,0)>0 then Class
      when isnull(PanelWxBrandUnits,0)>0 then Brand
      when isnull(PanelWxATC4Units,0)>0 then ATC4
      when isnull(PanelWxATC3Units,0)>0 then ATC3
      when isnull(PanelWxATC2Units,0)>0 then ATC2
      when isnull(PanelWxATC1Units,0)>0 then ATC1
      when isnull(PanelWxBasketUnits,0)>0 then Basket
      when isnull(PanelWxRxUnits,0)>0 then Rx
      else null
    end as Class,
    1 as Orphan
  from dbo.tmpPjf_099_OrphansWx
;

-- This is how we can use it with Panel data to e.g. check Projected Units per
-- Region.
-- IMPORTANT: "Date" field on [dbo].[TSMBackData] is very, very UGLY!
--
-- select Region, count(distinct ShopID) as No_of_Shops, count(distinct PackID) as No_of_Packs, sum(PrUnit) Units, sum(PrUnit*PF) ProjUnits
--   from IMSDAS_PROJECT_TURKEY_PHARMATREND_MONTHLY.dbo.TSMBackData
--   join dbo.tmpPjf_100_Pjf
--     on PdID = concat(substring(convert(varchar, cast ("Date" as date), 112),1,4),substring(convert(varchar, cast ("Date" as date), 112),7,2))
--    and RIGHT(REPLICATE('0', 7) + LEFT(ShopID, 7), 7) = RIGHT(REPLICATE('0', 7) + LEFT(IMSCustomerCode, 7), 7)
--    and RIGHT(REPLICATE('0', 8) + LEFT(PackID, 8), 8) = RIGHT(REPLICATE('0', 8) + LEFT(PFC, 8), 8)
--  group by Region
-- ;

--------------------------------------------------------------------------------
-- STEP#11 PROMOTE
-- Note:
-- Promote to a target database/table...
