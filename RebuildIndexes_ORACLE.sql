/*
   @<COPYRIGHT>@
   ===========================================================
   Copyright 2022
   Siemens Digital Industries Software.
   All Rights Reserved.
   ===========================================================
   @<COPYRIGHT>@
*/

/**********************************************************************************************************************
Description:	Oracle PL/SQL script to rebuild indices if necessary
Developer:	Peter Gartland
Date:		07/13/2022
Notes:		Assign appropriate values to variables
Siemens 
***********************************************************************************************************************/
set serveroutput on
set feedback off

--script variables:
declare  
 pOwner		varchar2(55) 	:= 'TCEMIGRATE';
 pDelPct	float    	:= 0.20;
 pHeight	smallint 	:= 3;
 pFillFactor	float		:= 0.70;

 type varchar_list is table of varchar2(255);
 TablesToShrink varchar_list := varchar_list();

 --sp to print to console:
 procedure PrintVal(x varchar2) is
 begin
  DBMS_OUTPUT.PUT_LINE(x);
 end;

 --sp to analyze index:
 procedure AnalyzeIndex(pOwner varchar2, pIndex_Name varchar2) is
 pStmt varchar(32767);
 begin
   pStmt := 'analyze index ' || pOwner || '.' || pIndex_Name || ' validate structure';
   execute immediate pStmt;  
 end;

 --fn to get adjusted fill factor:
 function FillFactor return float is
 begin
  return(1.00-pFillFactor);
 end;

 --sp to rebuild index, providing fill factor parameter:
 procedure RebuildIndex(pOwner varchar2, pIndex_Name varchar2) is
 pStmt varchar(32767);
 begin
  pStmt := 'alter index ' || pOwner || '.' || pIndex_Name || ' rebuild ' || 'pctfree ' || to_char(FillFactor()*100) || '';
  --PrintVal(pStmt);
  execute immediate pStmt;
  commit; 
  PrintVal('index ' || pIndex_Name || ' rebuilt');
 end;

 --fn to get max level of index tree:
 function Height return number is
 pHeight number;
 begin
  select Height
  into   pHeight
  from   sys.index_stats;
  return(pHeight);
 end;

 --fn to get percentage of deleted leaf nodes from index tree:
 function DelPct return float is
 pDelPct float := 0.0;
 begin
  select case when Br_Rows = 0 
              then 0.00
              else round(Del_Lf_Rows/(Lf_Rows*1.000),2)
         end
  into   pDelPct
  from   sys.index_stats;  
  return(pDelPct);
 end;

 --fn to see if index needs to be rebuilt(more rules may be added here):
 function NeedsRebuild return boolean is
 begin
  if(
    (Height() > pHeight) or 
    (DelPct() > pDelPct)
    ) 
    then return(true);
  end if;
  return(false);     
 end;

 --sp to print messages when rebuilding index:
 procedure PrintMsgs(pIndex_Name varchar2) is
 begin
  PrintVal(pIndex_Name  || ' analyzed');
  PrintVal('Height: '   || Height()); 
  PrintVal('DelPct: '   || DelPct()); 
 end; 
 
 --fn to check if value is in list:
 function InList(pList varchar_list, pVal varchar2) return boolean is
 begin
  for i in 1 .. pList.count() loop
   if(pList(i) = pVal) then 
    return(true);
   end if;
  end loop;
 return(false);
 end;

 --sp to add value to list:
 procedure Add(pList in out varchar_list, pVal varchar2) is 
 begin
  if not(InList(pList, pVal)) then
   pList.extend();
   pList(pList.count()) := pVal;
  end if;
 end;

 --sp to print list:
 procedure PrintList(pList varchar_list) is
 begin
  for i in 1 .. pList.count() loop
   PrintVal(pList(i));
  end loop;  
 end;

 --sp to apply action per each value in list:
 procedure ForEachIn(pList varchar_list, pStmt varchar2) is
 pStmt1 varchar2(32767) := pStmt;
 begin
  for i in 1 .. pList.count() loop
   pStmt1 := replace(pStmt, '{x}', pList(i));
   execute immediate pStmt1;
   commit;
  end loop;  
 end;

 --sp to rebuild some those indexes that need it:
 procedure RebuildSomeIndexes is
 begin
  --for each index, analyze it, then rebuild if necessary:
  for x in(select Owner,
		  Index_Name,
		  Table_Name		
	   from	  sys.all_indexes
	   where  Owner = pOwner
	   and 	  Num_Rows > 0) loop
   AnalyzeIndex(x.Owner, x.Index_Name);
   if(NeedsRebuild()) then   
    PrintMsgs(x.Index_Name);
    RebuildIndex(x.Owner, x.Index_Name);
    Add(TablesToShrink, x.Table_Name);
   end if; 
  end loop;
 end;

 --sp to shrink tables associated with rebuilt indexes:
 procedure ShrinkSomeTables is
 begin
  ForEachIn(TablesToShrink, 'alter table {x} enable row movement');
  ForEachIn(TablesToShrink, 'alter table {x} shrink space');
  PrintVal('Shrunk tables.....');
  PrintList(TablesToShrink);
 end;

--"main":
begin        
 RebuildSomeIndexes();
 ShrinkSomeTables();
end;
/

exit



