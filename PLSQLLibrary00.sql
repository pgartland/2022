/**********************************************************************************************************************
Description:	Lispy PL/SQL
Developer:	Peter Gartland
Date:		07/18/22
Notes:		-
***********************************************************************************************************************/
set serveroutput on
set feedback off
set pagesize 50000
set newpage 1
set linesize 700

declare  
 --types:
 type List    is table of varchar2(32767); 
 type LOL     is table of List;
 type HashTable is table of List index by varchar2(255);
 
 --variables:
 stmt		 varchar2(32767);
 tables 	 List;
 cols	 	 List;
 col		 List;
 col1		 List;
 col2		 List;
 col3		 List;
 hdrs1   	 List;
 hdrs2   	 List;
 widths  	 List;
 pgWidth	 smallint      := 115;
 lftMargin	 smallint      := 0;
 tbl1		 LOL;
 tbl2		 LOL;
 tbl3		 LOL;
 tbl4		 LOL;
 idx		 HashTable;

 --fn headers:
 function Add(lst in out List, val varchar2) return List;
 function Append(lst List, val varchar2) return List;
 function Append(lst1 List, lst2 List) return LOL;
 function Copy(lst List) return List;
 function ListToScalar(lst List) return varchar2;
 function InList(lst List, pVal varchar2) return boolean;
 function Get(lst List, n int) return varchar2;
 function GetPos(cols List, col varchar2) return smallint;
 function MaxLen(lst List) return smallint;
 function MaxCount(Tbl LOL) return int;
 function ColToList(owner varchar2, tbl varchar2, col varchar2, pWhere varchar2 default '', pOrderBy varchar2 default '', pTopN smallint default 20) return List;
 function ColsToLOL(owner varchar2, dbTbl varchar2, cols List, pWhere varchar2 default '', pOrderBy varchar2 default '', pTopN smallint default 20) return LOL;
 function HeaderLine(hdrs List, Tbl LOL) return varchar2;
 function Accum(lst List, op varchar2) return List;
 function DivBy2(x int) return int;
 function f2(lst List) return int;
 function cons(Tbl LOL, lst List) return LOL;
 function quote(pVal varchar2) return varchar2;
 function Long2Clob(pOwner varchar2, pOBID varchar2) return clob;
 function Min1(lst List) return varchar2;
 function Max1(lst List) return varchar2;
 function Locate(val varchar2, str varchar2, n smallint) return smallint;
 function Split(str varchar2, delim varchar2, subStr1 varchar2 default null, post boolean default true) return List;
 function MaxNonNullPos(lst List) return int;

 --sp headers:
 procedure Add(lst in out List, pVal varchar2, pOpt smallint default 0);
 procedure Add(Tbl in out LOL, lst List);
 procedure Add(tbl in out LOL, col List, hdrs in out List, hdr varchar2); 
 procedure ForEachIn(lst List, pStmt varchar2);
 procedure Put(x varchar2);
 procedure PrintVal(x varchar2);
 procedure PutList(lst List);	
 procedure PrintList(lst List);
 procedure PrintLOL(Tbl LOL, hdrs List);
 procedure PrintHeaders(hdrs List, Tbl LOL, hdrLine varchar2);
 procedure PrintTable(pOwner varchar2, tbl varchar2, hdrs in out List);
 procedure PrintHashTable(lkup HashTable);   
 procedure sp1(lst List);
 procedure sp2(params List);
 procedure sp3(Tbl LOL);
 procedure PrintHeaders(hdrs List, Tbl LOL, hdrLine varchar2, widths List);
 procedure AsciiCheck(str varchar2);


 --fn to convert List to single string (values separated by spaces):
 function ListToScalar(lst List) return varchar2 is
 val varchar2(32767) := '';
 begin
  for i in 1 .. lst.count() loop
   val := val || ' ' || lst(i);
  end loop;
 return(val);
 end;

 --fn to add list to index:
 function GetIndex(lst List) return HashTable is
 lkup HashTable;
 val varchar2(255);
 begin
  for i in 1 .. lst.count() loop
   val := lst(i);
   if (not (lkup.exists(val))) then
    lkup(val) := List(i);    
   else lkup(val) := Add(lkup(val), i);
   end if;
  val := null; 
  end loop;
  return(lkup);
 end;

 --sp to print a hash table (index):
 procedure PrintHashTable(lkup HashTable) is
 val varchar2(255);
 hdrs List := List('Value','Positions');
 lst1 List := List();
 lst2 List := List();
 begin
  val := lkup.first;
  while val is not null loop
   Add(lst1, val);
   Add(lst2, ListToScalar(lkup(val)));
   val := lkup.next(val);
  end loop;
 PrintLOL(LOL(lst1,lst2), hdrs);
 end;

 --sp to print to console:
 procedure PrintVal(x varchar2) is
 begin
  Put(rpad(chr(9), lftMargin, chr(9)));
  DBMS_OUTPUT.PUT_LINE(x);
 end;

 --sp to print to console:
 procedure PrintVal(x varchar2, y smallint) is
 begin
  DBMS_OUTPUT.PUT_LINE(x);
 end;

 --sp to put to console:
 procedure Put(x varchar2) is
 begin
  DBMS_OUTPUT.PUT(x);
 end;

 --fn to check if value is in lst:
 function InList(lst List, pVal varchar2) return boolean is
 begin
  for i in 1 .. lst.count() loop
   if(lst(i) = pVal) then 
    return(true);
   end if;
  end loop;
 return(false);
 end;

 --sp to add value to lst:
 procedure Add(lst in out List, pVal varchar2, pOpt smallint default 0) is 
 begin
   if(lst.count=0) then
   lst.extend();
   lst(lst.count()) := pVal;
   return; 
  end if;
  if (not(InList(lst, pVal)) or pOpt=1) then
   lst.extend();
   lst(lst.count()) := pVal;
  end if;
 end;

 --fn to add value to lst:
 function Add(lst in out List, val varchar2) return List is 
 lst1 List := lst;
 begin
   if(lst.count=0) then
    return(List(val));
   end if;
  lst1.extend();
  lst1(lst1.count()) := val;
  return(lst1);
 end;
 
 --sp to add list to list-of-LOL:
 procedure Add(Tbl in out LOL, lst List) is 
 begin
   Tbl.extend();
   Tbl(Tbl.count()) := lst;
 end;

 --fn to add list to list-of-LOL:
 function Add(Tbl LOL, lst List) return LOL is 
 Tbl1 LOL := Tbl;
 begin
  Tbl1.extend();
  Tbl1(Tbl.count()) := lst;
  return(Tbl1);
 end;

 --sp to print lst:
 procedure PrintList(lst List) is
 begin
  for i in 1 .. lst.count() loop
   PrintVal(lst(i));
  end loop;  
 end;

 --sp to put lst (horizontal):
 procedure PutList(lst List) is
 begin
  for i in 1 .. lst.count() loop
   Put(' | ');
   Put(lst(i));
  end loop;    
 PrintVal(' | ');
 end;

 --sp to apply action per each value in lst:
 procedure ForEachIn(lst List, pStmt varchar2) is
 pStmt1 varchar2(32767) := pStmt;
 begin
  for i in 1 .. lst.count() loop
   pStmt1 := replace(pStmt, '{x}', lst(i));
   execute immediate pStmt1;
   commit;
  end loop;  
 end;
 
 --sp to walk lst recursively:
 procedure sp1(lst List) is 
 procedure sub(n int) is 
 begin
  if(n is null) then
   return;
  end if; 
  PrintVal(lst(n)); 
  sub(lst.next(n));
 end;
 begin
  sub(1);
 end;
 
 --fn to walk lst recursively, returning scalar value:
 function f2(lst List) return int is
 pTemp int := 0;
 begin
  for i in 1 .. lst.count() loop
   pTemp := pTemp + lst(i);
  end loop;
 return(pTemp);
 end;
 
 --sp to test variable number of parameters:
 procedure sp2(params List) is
 paramCount smallint := params.count();
 begin
  PrintVal('paramCount: ' || paramCount);
 end;

 --sp to test LOL type:
 procedure sp3(Tbl LOL) is
 begin
  for i in 1 .. Tbl.Count() loop 
   PrintList(Tbl(i));
  end loop;
 end;

 --fn to get value at position:
 function Get(lst List, n int) return varchar2 is
 begin
  if(n > lst.last) then 
   return(' ');
  end if;
  if (not(lst.Exists(n))) then
   return(null);
  end if;
  return(nvl(trim(lst(n)),' '));
 end;

 --fn to get max value length in list:
 function MaxLen(lst List) return smallint is
 maxLen smallint := 0;
 len    smallint := 0;
 begin
  if(lst is null or lst.count() = 0) then
   return(0);
  end if;
  for i in 1 .. lst.count() loop
   len := length(Get(lst, i));
   if(len>maxLen) then
    maxLen := len;
   end if;
  end loop;
 return(maxLen);
 end;

 --fn to get max list count from list of LOL:
 function MaxCount(Tbl LOL) return int is
 maxCount  int := 0;
 countTemp int;
 begin
  for i in 1 .. Tbl.count() loop
   countTemp := Tbl(i).count();
   if(countTemp > maxCount) then
    maxCount := countTemp;
   end if;
  end loop;
 return(maxCount);
 end;

 --fn to append value to list:
 function Append(lst List, val varchar2) return List is
 lst1 List := lst;
 begin
  lst1.extend();
  lst1(lst1.count()) := val;
  return(lst1);
 end;

 --fn to combine 2 lists in "table":
 function Append(lst1 List, lst2 List) return LOL is
 Tbl LOL := LOL();
 begin
  Tbl.extend(2);
  Tbl(1) := lst1;
  Tbl(2) := lst2;
  return(Tbl);
 end;

 --fn to append 2 Lists:
 function Append1(lst1 List, lst2 List) return List is
 lst List := lst1;
 begin
  for i in 1 .. lst2.count loop
   Add(lst,lst2(i),1);
  end loop;
  return(lst);
 end;

 --fn to append list to LOL:
 function cons(Tbl LOL, lst List) return LOL is
 Tbl1 LOL := Tbl;
 begin
  Tbl1.extend(1);
  Tbl1(Tbl1.count()) := lst;
  return(Tbl1);
 end;

 --fn to print header/footer line:
 function HeaderLine(Tbl LOL) return varchar2 is
 width  smallint := 0;
 line   smallint := 0;
 begin
  for i in 1 .. Tbl.count() loop
   width  := MaxLen(Tbl(i));  
   line := line + width;   
  end loop;
  line := line + 3*Tbl.count()-1;
  return('+' || rpad('-', line, '-') || '+');
 end;

 --fn to print header/footer line:
 function HeaderLine(hdrs List, Tbl LOL) return varchar2 is
 width  smallint := 0;
 line   smallint := 0;
 begin
  for i in 1 .. hdrs.count() loop
   width  := MaxLen(Append(Tbl(i), Get(hdrs, i)));  
   line := line + width;   
  end loop;
  line := line + 3*hdrs.count()-1;
  return('+' || rpad('-', line, '-') || '+');
 end;

 --fn to print header/footer line:
 function HeaderLine1(hdrs List, Tbl LOL) return varchar2 is
 width  smallint := 0;
 line   smallint := 0;
 begin
  for i in 1 .. hdrs.count() loop
   width  := length(hdrs(i));  
   line := line + width;   
  end loop;
  line := line + 3*hdrs.count()-1;
  return('+' || rpad('-', line, '-') || '+');
 end;

 --fn to print header/footer line:
 function HeaderLine2(hdrs List, Tbl LOL, widths List) return varchar2 is
 width  smallint := 0;
 line   smallint := 0;
 begin
  for i in 1 .. hdrs.count() loop
   width  := widths(i); --length(hdrs(i));  
   line := line + width;   
  end loop;
  line := line + 3*hdrs.count()-1;
  return('+' || rpad('-', line, '-') || '+');
 end;
 
 --sp to print headers:
 procedure PrintHeaders(hdrs List, Tbl LOL, hdrLine varchar2) is
 width   smallint := 0;
 line    smallint := 0;
 begin 
  if(hdrs.count() <> Tbl.count()) then
   PrintVal('# of headers <> # of columns');
   return;
  end if;
  PrintVal(hdrLine);
  Put(rpad(chr(9), lftMargin, chr(9)));
  Put('| ');
  for i in 1 .. hdrs.count() loop
   width  := MaxLen(Append(Tbl(i), Get(hdrs, i)));  
   width := width - length(Get(hdrs, i));    
   Put(Get(hdrs, i) || rpad(' ', width, ' '));   
   Put(' | ');
  end loop;
  PrintVal(rpad(chr(32), hdrs.count(), chr(32)) /*|| '| '*/); 
  PrintVal(hdrLine);
 end;

 --sp to print headers (overload)
 procedure PrintHeaders(hdrs List, Tbl LOL, hdrLine varchar2, widths List) is
 width   smallint := 0;
 line    smallint := 0;
 pos     int      := 1;
 maxiLen   smallint := MaxLen(hdrs);
 miniWidth smallint := Min1(widths);
 maxiWidth smallint := Max1(widths);
 subRows  smallint;
 procedure PrintRow(hdrs List, r int, widths List, pos int)  is
 val       varchar2(32767);
 width     smallint;
 begin
  Put(rpad(chr(9), lftMargin, chr(9)));
  Put('| ');
  for c in 1 .. hdrs.count() loop  		
   width := widths(c);
   if(length(substr(Get(hdrs, c), 1)) < widths(c) and pos > 1) then
    val := '';
   else
    val := substr(Get(hdrs, c), pos, width);
   end if;
   Put(substr(val || rpad(' ', width, ' '), 1, width));     
   Put(' | ');
  end loop;    
  PrintVal(' ',1);  
 end;
 procedure PrintRow1(hdrs List, r int, widths List, subRows smallint, i smallint)  is
 val    varchar2(32767);
 width  smallint;
 pos 	int;
 begin
  Put(rpad(chr(9), lftMargin, chr(9)));
  Put('| ');
  for c in 1 .. hdrs.count() loop  		
   width := widths(c);
   pos   := (width * i - width + 1);
   if(length(substr(Get(hdrs, c), 1, width)) < widths(c) and pos > 1) then
    val := '';
   else
    val := substr(Get(hdrs, c), pos, width);
   end if; 
   Put(substr(trim(val) || rpad(' ', width, ' '), 1, width));     
   Put(' | ');
  end loop;    
  PrintVal(' ',1);
 end;
 function MaxLen1(Tbl LOL, r int) return int is
 maxLen int := 0;
 begin
  for i in 1 .. Tbl.count() loop
   if(length(Tbl(i)(r)) > maxLen) then
    maxLen := length(Tbl(i)(r));
   end if;
  end loop;
 return(maxLen);
 end;
 function SubRows1(Tbl LOL, r int, widths List) return int is
 len     int;
 width   smallint;
 subRows smallint := 0;
 begin
  for i in 1 .. Tbl(r).count() loop
   len   := length(Tbl(r)(i));
   width := widths(i);
   if((len/width) > subRows) then
    subRows := ceil((len*1.00)/(width*1.00));
   end if;
  end loop;
 return(subRows);
 end;
 begin 
  if(hdrs.count() <> Tbl.count()) then
   PrintVal('# of headers <> # of columns');
   return;
  end if;
  PrintVal(hdrLine);  
  pos := 1;
  subRows := SubRows1(LOL(hdrs), 1, widths) + 0;
  for i in 1 .. subRows loop
   PrintRow1(hdrs, i, widths, subRows, i);   
   pos := pos + maxiWidth;
  end loop;
  PrintVal(hdrLine);  
 end;

 --fn to get max value from list:
 function Max1(lst List) return varchar2 is
 val 	 varchar2(32767) := '0';
 valTemp varchar2(32767) ;
 begin
  for i in 1 .. lst.count() loop
   valTemp := lst(i);
   if(valTemp > val) then
    val := valTemp;
   end if;
  end loop;
 return(val);
 end; 

 --fn to get min value from list:
 function Min1(lst List) return varchar2 is
 val 	 varchar2(32767);
 valTemp varchar2(32767);
 begin
  for i in 1 .. lst.count() loop
   valTemp := lst(i);
   if(valTemp <= nvl(val,valTemp)) then
    val := valTemp;
   end if;
  end loop;
 return(val);
 end; 

 --sp to print list of LOL:
 procedure PrintLOL(Tbl LOL, hdrs List) is
 width smallint := 0;
 val   varchar2(255);
 hdrLine varchar(32767); 
 r int;
 c int;
 begin
  if(hdrs.count() <> Tbl.count()) then
   PrintVal('# of headers <> # of columns');
   return;
  end if;
  hdrLine := HeaderLine(hdrs, Tbl);
  PrintHeaders(hdrs, Tbl, hdrLine);
  r := Tbl(Tbl.first).first;
  while (r is not null) loop 
   c := Tbl.first;
   while (c is not null) loop
    width := MaxLen(Append(Tbl(c), hdrs(c)));
    val := Get(Tbl(c), r);
    --width := width - length(val);
    Put(' | ');
    Put(substr(trim(val) || rpad(' ', width, ' '),1,width)); 
    c := Tbl.next(c);
   end loop;  
   PrintVal(' | ');
   r := Tbl(Tbl.first).next(r);
  end loop;
  PrintVal(hdrLine);
 end;

 procedure PrintLOL(Tbl LOL) is
 rows  int := MaxCount(Tbl); 
 cols  int := Tbl.Count();
 width smallint := 0;
 val   varchar2(255);
 hdrLine varchar(32767);
 begin
  hdrLine := HeaderLine(Tbl);
  PrintVal(hdrLine);
  for r in 1 .. rows loop 
   for c in 1 .. cols loop
    width := MaxLen(Tbl(c));
    val := Get(Tbl(c), r);
    width := width - length(val);
    Put(' | ');
    Put(val || rpad(' ', width, ' ')); 
   end loop;  
   PrintVal(' | ');
  end loop;
  PrintVal(hdrLine);
 end;

 --sp to print list of LOL (overload):
 procedure PrintLOL(Tbl LOL, hdrs List, widths List) is
 rows    int 		:= MaxCount(Tbl);   
 cols    int 		:= Tbl.Count();
 width   smallint 	:= 0;
 val     varchar2(255)	;
 hdrLine varchar(32767)	;
 pos 	 int		;
 procedure PrintRow(hdrs List, r int, widths List, pos int)  is
 val   varchar2(32767);
 width smallint;
 begin
  for c in 1 .. cols loop  		
   width := widths(c);
   val := substr(Get(Tbl(c), r), pos, width);
   Put(' | ');
   Put(substr(val || rpad(' ', width, ' '), 1, width)); 
  end loop;  
  PrintVal(' | '); 
 end;
 begin
  if(hdrs.count() <> Tbl.count()) then
   PrintVal('# of headers <> # of columns');
   return;
  end if;
  hdrLine := HeaderLine2(hdrs, Tbl, widths);
  PrintHeaders(hdrs, Tbl, hdrLine, widths);
  for r in 2 .. rows loop 
   pos := 1;
   while 100/pos > 1 loop
    PrintRow(hdrs, r, widths, pos);
    pos := pos + 20;
   end loop;
  end loop;
  PrintVal(hdrLine);
 end;

 --sp to print list of LOL (overload):
 procedure PrintLOL1(Tbl LOL, hdrs List, widths List) is
 rows      int 		  := MaxNonNullPos(Tbl(1));    
 cols      int 		  := Tbl.Count();
 width     smallint 	  := 0;
 val       varchar2(255)  ;
 hdrLine   varchar(32767) ;
 pos       int            := 1;
 maxiLen   smallint       := MaxLen(hdrs);
 miniWidth smallint       := Min1(widths);
 maxiWidth smallint       := Max1(widths);
 subRows   smallint;
 procedure PrintRow(hdrs List, r int, widths List, pos int)  is
 val       varchar2(32767);
 width     smallint;
 begin
  for c in 1 .. hdrs.count() loop  		
   width := widths(c);
   if(length(substr(Get(Tbl(c), r), 1, width)) < widths(c) and pos > 1) then
    val := '';
   else
    val := substr(Get(Tbl(c), r), pos, width);
   end if;
   Put(' | ');
   Put(substr(val || rpad(' ', width, ' '), 1, width));     
  end loop;    
  PrintVal(' | ');  
 end;
 procedure PrintRow1(hdrs List, r int, widths List, subRows smallint, i smallint)  is
 val    varchar2(32767);
 width  smallint;
 pos 	int;
 begin
  Put(rpad(chr(9), lftMargin, chr(9)));
  Put('| ');
  for c in 1 .. hdrs.count() loop  		
   width := widths(c);
   pos   := (width * i - width + 1);
   if(length(substr(Get(Tbl(c), r), 1, width)) < widths(c) and pos > 1) then
    val := '';
   else
    val := substr(Get(Tbl(c), r), pos, width);
   end if; 
   Put(substr(trim(val) || rpad(' ', width, ' '), 1, width));     
   Put(' | ');
  end loop;    
  PrintVal(' ',1);
 end;
 function MaxLen1(Tbl LOL, r int) return int is
 maxLen int := 0;
 begin
  for i in 1 .. Tbl.count() loop
   if(length(Tbl(i)(r)) > maxLen) then
    maxLen := length(Tbl(i)(r));
   end if;
  end loop;
 return(maxLen);
 end;
 function SubRows1(Tbl LOL, r int, widths List) return int is
 len     int;
 width   smallint;
 subRows smallint := 0;
 begin
  for i in 1 .. Tbl.count() loop
   len   := length(Tbl(i)(r));
   width := widths(i);
   if((len/width) > subRows) then
    subRows := (len/width);
   end if;
  end loop;
 return(subRows);
 end;
 begin
  if(hdrs.count() <> Tbl.count()) then
   PrintVal('# of headers <> # of columns');
   return;
  end if;
  hdrLine := HeaderLine2(hdrs, Tbl, widths);
  PrintHeaders(hdrs, Tbl, hdrLine, widths);
  for r in 2 .. rows loop  
   pos := 1;
   subRows := SubRows1(Tbl, r, widths) + 1;
   for i in 1 .. subRows loop
    PrintRow1(hdrs, r, widths, subRows, i);
    pos := pos + maxiWidth;
   end loop;
  PrintVal(hdrLine);
  end loop;
 end;

 --fn to convert column to list:
 function ColToList(owner varchar2, tbl varchar2, col varchar2, pWhere varchar2 default '', pOrderBy varchar2 default '', pTopN smallint default 20) return List is
 lst  List := List();
 stmt varchar2(32767);
 begin
  stmt := 'select ' || col || ' from ' || owner || '.' || tbl || ' ' || pWhere || ' ' || pOrderBy || 'fetch first ' || pTopN || ' rows only';
  execute immediate stmt bulk collect into lst;
  return(lst);
 end; 

 --fn to convert column to list:
 function ColsToLOL(owner varchar2, dbTbl varchar2, cols List, pWhere varchar2 default '', pOrderBy varchar2 default '', pTopN smallint default 20) return LOL is
 Tbl  LOL := LOL();
 begin
  for i in 1 .. cols.count() loop
   Tbl.extend();
   Tbl(Tbl.count()) := ColToList(owner, dbTbl, cols(i), pWhere, pOrderBy, pTopN);
  end loop;
  return(Tbl);
 end; 
 
 --sp to print table:
 procedure PrintTable(pOwner varchar2, tbl varchar2, hdrs in out List) is
 Tbl1 LOL := LOL();
 begin
   for y in (select distinct
	            a.column_name
	     from   sys.all_tab_columns a
	     join   sys.all_tables b
	     on	    a.owner = b.owner 
             and    b.owner = upper(pOwner)
	     and    a.table_name = b.table_name
	     and    b.table_name = upper(tbl)
	     order by a.column_name) loop
   Tbl1.extend();
   Tbl1(Tbl1.count()) := ColToList(pOwner, tbl, y.column_name);     
   Add(hdrs, y.column_name);
  end loop;
  PrintLOL(Tbl1, hdrs);
 end;

 --sp to print table:
 procedure PrintTable(pOwner varchar2, tbl varchar2) is
 hdrs List := List();
 Tbl1 LOL := LOL();
 begin
   for y in (select distinct
	            a.column_name
	     from   sys.all_tab_columns a
	     join   sys.all_tables b
	     on	    a.owner = b.owner 
             and    b.owner = upper(pOwner)
	     and    a.table_name = b.table_name
	     and    b.table_name = upper(tbl)
	     order by a.column_name) loop
   Tbl1.extend();
   Tbl1(Tbl1.count()) := ColToList(pOwner, tbl, y.column_name);     
   Add(hdrs,y.column_name,1);  
  end loop;  
  PrintLOL(Tbl1,hdrs);
 end;

 --fn to get position number of column, based on header list:
 function GetPos(cols List, col varchar2) return smallint is
 begin
  for i in 1 .. cols.count() loop
   if(cols(i) = col) then
    return(i);
   end if;
  end loop;
 return(null);
 end;

 function DivBy2(x int) return int is 
 begin
  return(x/2);
 end;

 --fn to walk lst recursively, returning new lst:
 function Squared(lst List) return List is 
 lst1 List := List(); 
 function f(x int) return int is 
 begin
  return(x*x);
 end;
 begin
  for i in 1 .. lst.count() loop
   Add(lst1,f(lst(i)),1);
  end loop;
 return(lst1);
 end; 
  
 --fn to walk lst recursively, returning new lst:
 function Accum(lst List, op varchar2) return List is 
 lst1 List := List(); 
 x int;
 function Init(op char) return int is
 begin
   case op
   when '+'  then return(0);
   when '-'  then return(0);
   when '*'  then return(1);
   when '/'  then return(1);
   when '**' then return(1);
   when '||' then return('');
  end case;
 end;
 function f(x int, y int) return int is
 begin
  case op
   when '+'  then return(x+y);
   when '-'  then return(x-y);
   when '*'  then return(x*y);
   when '/'  then return(x/y);
   when '**' then return(x**y);
   when '||' then return(x||y);
  end case;
 end;
 begin
  x := Init(op);
  for i in 1 .. lst.count() loop
   x := f(lst(i),x);
   Add(lst1,x,1);
  end loop;
 return(lst1);
 end; 
 
 --fn to walk lst recursively, returning new lst:
 function Accum1(lst List, op varchar2) return List is 
 lst1 List := List(); 
 x int;
 function Init(op char) return int is
 begin
   case op
   when '+'  then return(0);
   when '-'  then return(0);
   when '*'  then return(1);
   when '/'  then return(1);
   when '**' then return(1);
  end case;
 end;
 function f(x int, y int) return int is
 begin
  case op
   when '+'  then return(x+y);
   when '-'  then return(x-y);
   when '*'  then return(x*y);
   when '/'  then return(x/y);
   when '**' then return(x**y);
  end case;
 end;
 begin
  x := Init(op);
  for i in 1 .. lst.count() loop
   x := f(lst(i),x);
  end loop;
  for i in 1 .. lst.count loop
   Add(lst1,x,1);
  end loop;
 return(lst1);
 end; 

 --fn to copy list:
 function Copy(lst List) return List is
 lst1 List := List();
 begin
  for i in 1 .. lst.count() loop
   Add(lst1, Get(lst, i));
  end loop;
 return(lst1);
 end;

 --sp to add col to tbl:
 procedure Add(tbl in out LOL, col List, hdrs in out List, hdr varchar2) is
 begin
  Add(hdrs, hdr);
  Add(tbl, col);
 end;

 --function to quote strings:
 function quote(pVal varchar2) return varchar2 is
 begin
  return(chr(39) || pVal || chr(39));
 end;

 --fn to convert ZBINDB.ZBINDATA long value to clob value:
 function Long2Clob(pOwner varchar2, pOBID varchar2) return clob is
 pCsr    binary_integer;
 pStmt   varchar2(1000) := 'select ZBINDATA from ' || pOwner || '.ZBINDB where OBID = ' || quote(pOBID) ;
 pPiece  varchar2(32767);
 pClob   clob;
 pPlen   int := 32767;
 pTlen   int := 0;
 pRows   int;
 begin
  begin
   pCsr  := dbms_sql.open_cursor;    
   dbms_sql.parse(pCsr, pStmt, dbms_sql.native);
   dbms_sql.define_column_long(pCsr, 1);
   pRows := dbms_sql.execute_and_fetch(pCsr);
   if(pRows is null) then 
    PrintVal('pRows: ' || pRows);
    return(null);
   end if;
    loop
     dbms_sql.column_value_long(pCsr, 1, 32767, pTlen, pPiece, pPlen);
     pClob := pClob || pPiece;
     pTlen := pTlen + 32767;
     exit when pPlen < 32767;
    end loop;	
    dbms_sql.close_cursor(pCsr);
    exception 
    when others then        
    if(sqlcode = -1016) then
     PrintVal('record not found (Long2Clob)');
     return(null);
    else         
     PrintVal('ERROR in Long2Clob: OBID' || pOBID);          
     raise;
     return(null);   
    end if; 	  
   end;
  return(pClob);
 end Long2Clob;

 --fn to get position of n-th occurrence of val in string:
 function Locate(val varchar2, str varchar2, n smallint) return smallint is
 occur smallint := 0;
 pos   smallint := 1;
 len   smallint := length(str);
 begin
  if(n<1) then 
   return(0);
  end if;
  while(pos<>0) loop
   pos := instr(substr(str, pos), val);
   if(pos=0) then
    return(pos);
   end if;  
   if(pos>0) then 
    occur := occur+1;   
   end if;  
   if(occur=n) then
     return(pos + length(val));
   end if;  
  pos := pos + length(val);
  end loop;
  return(0); 
 end; 
 
 --fn to get a List from a string, using delimiter to split string:
 function Split(str varchar2, delim varchar2, subStr1 varchar2 default null, post boolean default true) return List is
 lst   List     	:= List();			--list to be returned
 len   smallint 	:= length(str); 		--length of str
 len1  smallint 	:= nvl(length(delim),0);	--length of delim
 str1  varchar2(32767)  := '';				--substring of str
 cN    varchar(25) 	:= '';				--running block of N chars
 c     char		;                		--holds 1 char at a time
 j     smallint 	:= 0;                   	--running length of substring
 p     smallint 	:= nvl(instr(str, subStr1),1); 	--starting position
 val   varchar2(32767)  ;
 begin
  if(post) then
   p := p + nvl(length(subStr1),0);
  end if;
  for i in p .. len loop
   j := j+1;
   c := substr(str,i,1);
   str1 := str1 || c;
   cN   := cN || c;
   if(cN = delim) then
    val := substr(str1, 1, j-len1);
    Add(lst, val, 1);
    str1 := '';
    cN   := '';
    j := 0;
   end if;
   if(length(cN) = len1) then
    cN := substr(cN,2);
   end if;
  end loop; 
  return(lst);
 end;
 
 --sp to get ascci vals of all chars in str:
 procedure AsciiCheck(str varchar2) is
 len smallint := length(str);
 c   char;
 begin
  for i in 1 .. len loop
   c := substr(str,i,1);
   PrintVal(c || ':' || ascii(c));
  end loop;
 end;

 --fn to get max non-null position in list:
 function MaxNonNullPos(lst List) return int is
 pos int := 0;
 begin
  for i in 1 .. lst.count() loop
   if(length(lst(i)) > 0) then
    pos := i;
   end if;
  end loop;
 return(pos);
 end;
 
 function Copy(Tbl LOL) return LOL is
 Tbl1 LOL := LOL();
 begin
  for ColNum in 1 .. Tbl.count() loop
   Add(Tbl1,Tbl(ColNum));
  end loop;
  return(Tbl1);
 end;
 
 --sp to add element to each List:
 procedure AddRow(Tbl in out LOL, Row List) is
 begin
  for ColNum in 1 .. Row.count loop
   Add(Tbl(ColNum),Row(ColNum),1);
  end loop;
 end;

 --function to get row from LOL:
 function GetRow(Tbl LOL, RowNum int) return List is
 Row List := List();
 begin
  for ColNum in 1 .. Tbl.count loop
   Add(Row,Tbl(ColNum)(RowNum),1);
  end loop;
  return(Row);
 end;
 
 --fn to join on 2 columns, and return a 3rd:
 function Join(TblA LOL, TblB LOL, ColA smallint, ColB smallint, TblBcols List) return LOL is
  Tbl       	LOL     	:= LOL();
  Idx      	HashTable 	:= GetIndex(TblB(ColB));
  Row      	List      	;
  RowTemp   	List     	;
  TblA_RowCount int 		:= TblA(ColA).count;
  TblB_ColCount int 		:= TblBcols.count;
  procedure Initialize(Tbl in out LOL, TblA LOL, TblB LOL) is
   Tbl_ColCount smallint := (TblA.count + TblBcols.count);  
   begin
    for i in 1 .. Tbl_ColCount loop
     Add(Tbl,List());
    end loop;
   end; 
  function TblB_Rows(TblA_RowNum int) return List is
   function Idx_Val(Key varchar2) return List is
    begin
     return(Idx(Key));
    end;
   function TblA_Val(TblA_RowNum int) return varchar2 is
    begin
     return(TblA(ColA)(TblA_RowNum));
    end;
   begin
    return(Idx_Val(TblA_Val(TblA_RowNum)));
   end;
  function TblB_RowCount(TblA_RowNum int) return smallint is
  begin
   return(TblB_Rows(TblA_RowNum).count);
  end;
  function TblB_Val(TblA_RowNum int, TblB_RowNum int, TblB_ColNum int) return varchar2 is
  begin
   return(TblB(TblBcols(TblB_ColNum))(TblB_Rows(TblA_RowNum)(TblB_RowNum)));
  end;
  begin
   Initialize(Tbl,TblA,TblB);
   for TblA_RowNum in 1 .. TblA_RowCount loop  
    Row := GetRow(TblA,TblA_RowNum);     
    for TblB_RowNum in 1 .. TblB_RowCount(TblA_RowNum) loop           
     RowTemp := Row;
     for TblB_ColNum in 1 .. TblB_ColCount loop  
      RowTemp := Append(RowTemp,TblB_Val(TblA_RowNum,TblB_RowNum,TblB_ColNum));                       
     end loop;
     AddRow(Tbl,RowTemp);
    end loop;
   end loop; 
   return(Tbl);
  end; 

 function InitLOL(ColCount smallint) return LOL is
 Tbl LOL := LOL();
 begin
  for i in 1 .. ColCount loop
   Add(Tbl,List());
  end loop;
  return(Tbl);
 end;
 
 function Filter(Tbl LOL, ColNum smallint, Vals List) return LOL is
 Tbl1 LOL := InitLOL(Tbl.count);
 begin
  for i in 1 .. Tbl(ColNum).count loop
   if(InList(Vals,Tbl(ColNum)(i))) then
    AddRow(Tbl1,GetRow(Tbl,i));
   end if;
  end loop; 
 return(Tbl1);
 end;


--"main":
begin
 i:= 0;      
 for puid in (select distinct ItemRevPUID from migration9.PG_Chain1) loop
   i := (i+1);
   PrintVal('i: ' || i);
   if(i>10) then
    exit;
   end if;
 end loop;


 null;
end;
/






