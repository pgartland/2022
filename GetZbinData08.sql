/**********************************************************************************************************************
Description:	Oracle PL/SQL script to convert and unpack TcE ZBINDB.ZBINDATA data
Developer:	Peter Gartland
Date:		04/04/2022
Notes:		(1) Single "long" column table assumed: ZBINDB; can be edited/parameterized if different.
		(2) Delimiter of NULL char (ascii 0) assumed; can be edited/parameterized if different.
***********************************************************************************************************************/
set serveroutput on
set feedback off
set verify off
alter session enable parallel dml;

declare  
  pPrintNum float 	:= 0.1;
  pClob     clob;

  --procedure to print value to console:
  procedure PrintVal(x varchar2) is
  begin
    begin
      DBMS_OUTPUT.PUT_LINE(x);
      exception 
      when others then        
        PrintVal('ERROR in PrintVal (varchar2)');
        raise;
        return;   
    end;
  end;
 
  --procedure to print on pPrintNum condition:
  procedure PrintVal(x varchar2, pPrintNum1 float, pPrintNum2 float) is
  begin
    begin
      if(pPrintNum1=0) then
        PrintVal(x);
      else if(pPrintNum1=pPrintNum2) then
             PrintVal(x);
           end if;
      end if;    
      exception 
      when others then        
        PrintVal('ERROR in PrintVal (varchar2, float, float)');
        raise;
        return;   
    end;
  end;

  --procedure to print large clob values:
  procedure PrintVal(x clob, pPrintNum1 float, pPrintNum2 float) is
  pLen 		int := dbms_lob.getlength(x);
  pOffSet	int :=1;
  pNumChars	int := 32767;
  begin    
    begin
      if(pPrintNum1=0 or pPrintNum1=pPrintNum2) then
        loop	 
          PrintVal(dbms_lob.substr(x, pNumChars, pOffSet));  
          pOffSet:=(pOffSet+pNumChars);
          if(pOffSet > pLen) then
            exit;
          end if;
        end loop;
      end if;
      exception 
        when others then        
          PrintVal('ERROR in PrintVal (clob, float, float)');
          raise;
          return;   
    end;
  end;
    
  --function to quote strings:
  function quote(pVal varchar2) return varchar2 is
  begin
      return('''' || pVal || '''');     
  end;
 
  --function to add leading and trailing commas to list:
  function DelimWrap(pVal varchar2, pDelim char) return varchar2 as
  pFirst char := substr(pVal,1,1);
  pLast  char := substr(pVal,length(trim(pVal)),1);
  begin
    if (pFirst <> pDelim and pLast <> pDelim) then 
      return(pDelim || pVal || pDelim);
    end if;
    if (pFirst = pDelim and pLast <> pDelim) then 
      return(pVal || pDelim);
    end if;
    if (pFirst <> pDelim and pLast = pDelim) then 
      return(pDelim || pVal);
    end if;
    if (pFirst = pDelim and pLast = pDelim) then 
      return(pVal);
    end if;
  end;

  --function to get count of records in clob value:
  function GetRecordCount(pList varchar2) return int as 
  begin
    return(length(DelimWrap(pList,','))-length(replace(DelimWrap(pList,','),',',''))-2);
  end GetRecordCount;

  --function to get value at specified position in VARCHAR value:
  function ValueAt(pVal varchar2, i int) return varchar2 as
  pStart int := instr(DelimWrap(pVal,','),',',1,i)+1;
  pEnd   int := instr(DelimWrap(pVal,','),',',pStart,1)-1;
  begin
    return(substr(DelimWrap(pVal,','),pStart,pEnd-pStart+1));
  end ValueAt; 
  
  --procedure to make table of given ZBINDB.ZBINDATA property data:
  procedure MakeTable(pOwner varchar2, pTable varchar2, pColumns varchar2) is
  pStmt 	varchar2(32767);
  pNumColumns	smallint := (GetRecordCount(DelimWrap(pColumns,','))+1);
  pColumns01	varchar2(4000) := DelimWrap(pColumns,',');
  pCol		varchar2(255);
  begin 
    begin
      pStmt := 'DROP TABLE ' || pOwner || '.' || pTable;	
      execute immediate pStmt;
      commit;
    exception 
      when others then 
        if sqlcode != -942 then
          raise;
  	  return; 
        end if;
    end;
    begin
      pStmt := 'create table ' || pOwner || '.' || pTable || '(OBID varchar2(24), ID int';                  
      for i in 1 .. pNumColumns loop
        pStmt := pStmt || ',' || ValueAt(pColumns01,i) || ' varchar2(4000)';	 
      end loop;
      pStmt := pStmt || ') nologging parallel';
      execute immediate pStmt;
    exception 
      when others then 
        if sqlcode != -942 then
          raise;
  	  return;
        end if;
    end;
  end;
   
  --function to get cursor:
  function GetCursor1(pOwner varchar2, pTable varchar2, pColumns varchar2, pProperty varchar2, pTop int default NULL) return sys_refcursor as
  pCursor	sys_refcursor; 
  pWhere   	varchar2(255) := '';
  pStmt	    	varchar2(32767);
  pOrderBy	varchar2(200)  := ' order by OBID ';
  begin
    begin
      if(length(pProperty)>0) then 
        pWhere := ' and a.' || pProperty || '=''+''';
      end if;    
      pStmt :=  ' select a.' || pColumns || 
		' from '     || pOwner   || '.' || pTable || ' a ' ||
		' join '     || pOwner   || '.' || 'ZBINDB b '     ||
                ' on a.OBID = b.OBID '   ||
                  pWhere     || 
                  pOrderBy;	
      if(pTop is not null) then 
        pStmt := pStmt || ' fetch first ' || to_char(pTop) || ' rows only ';
      end if;      
      open pCursor for pStmt; 
      exception 
        when others then        
 	  PrintVal('ERROR in GetCursor',1,1);
          raise;
  	  return(null);       
    end;    
    return(pCursor);
  end;

  --function to get cursor for specific OBID:
  function GetCursor2(pOwner varchar2, pTable varchar2, pOBID varchar2) return sys_refcursor as
  pCursor	sys_refcursor; 
  pWhere   	varchar2(255) := '';
  pStmt	    	varchar2(32767);
  begin
    begin
      pWhere := ' where OBID = ' || quote(pOBID);
      pStmt := ' select OBID from ' || pOwner || '.' || pTable || pWhere;
      open pCursor for pStmt; 
      exception 
        when others then        
 	  PrintVal('ERROR in GetCursor',1,1);
          raise;
  	  return(null);       
    end;
    return(pCursor);
  end;
  
  --function to convert long value to clob value:
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
        PrintVal('pRows: ' || pRows,1,1);
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
 	    PrintVal('ERROR in Long2Clob: OBID' || pOBID,1,1);          
            raise;
	    return(null);   
          end if; 	  
    end;
    return(pClob);
  end Long2Clob;

  --function to get length of sub-clob value, when specified:
  function GetSubClobLength(pClob clob, pProperty varchar2) return int is
  i int := dbms_lob.instr(lower(pClob),lower(pProperty),1,1);
  j int := (regexp_instr(lower(pClob),'b[0-9]',i+1,1)+1);
  k varchar2(6) := '';
  begin    
    if(j=1) then
      return(0);
    end if;
    while(dbms_lob.substr(pClob,1,j) in('0','1','2','3','4','5','6','7','8','9')) loop
      k := k || dbms_lob.substr(pClob,1,j);
      j := (j+1);
    end loop;
    if(k='') then
      return(0);
    end if;
  return(to_number(k)+2);  
  end;
  
  --function to get long data for specific property in clob format:
  function GetZbinData(pClob clob, pProperty varchar2) return clob as
  pOffSet     	int  	 	:= dbms_lob.instr(lower(pClob),lower(pProperty),1,1);
  pLen	    	int  	 	:= 0;
  pNumChars 	int     	; 
  pReturn 	clob		:= to_clob('?');
  pNPos		int		;
  begin      
    begin  
      if(pOffSet=0) then 
        return(null); 
      end if;
      pOffSet 	:= (pOffSet+length(pProperty)-1);    

      if(pLen=0) then 
        pLen := dbms_lob.instr(pClob,'kstop',pOffSet,1);
        pOffSet := pOffSet+1;
	pNumChars := (pLen-pOffSet);
      end if;

      if(pLen=0) then 
        pLen := GetSubClobLength(pClob,pProperty);   
        pOffSet := (pOffSet + length(to_char(pLen))+2);     
	pNumChars := (pLen+0);          
      end if;

      if(pLen=0) then 
        pLen := dbms_lob.getlength(pClob);  
	pNumChars := (pLen-pOffSet);
      end if;      

      dbms_lob.copy(pReturn, pClob, pNumChars, 1, pOffSet);    
      
      exception 
        when others then        
          PrintVal('ERROR in GetZbinData',1,1);
          raise;
  	  return(null);    
    end;   
    return(pReturn);
  end GetZbinData;

  --function to list of positions of delimiter occurrences in clob value:
  function GetDelimPos(pVal clob) return varchar2 as
  pAsciiNum 	int		:= 0; --the "NULL" character is always used as a delimiter in ZBINDB.ZBINDATA 
  pLen  	int 		:= length(pVal);
  i    	 	int 		:= 1;
  j		int		:= 0;
  pDelimPos	varchar2(32767) := ',';
  begin
   begin
      if(dbms_lob.getlength(pVal) = 2 and  dbms_lob.substr(pVal,1,1) = 's' and ascii(dbms_lob.substr(pVal,1,2)) = 0) then 
        return(null);
      end if;

      while (i<=pLen) loop
        if(ascii(dbms_lob.substr(pVal,1,i)) = pAsciiNum) then
          pDelimPos := pDelimPos || i || ',';
          j:=(j+1);
        end if;
        i:=(i+1);
      end loop;  

      if(j=0) then
        pDelimPos := ',' || '1,' || to_char(pLen) || ',';
       	return(pDelimPos);
      end if;

      if(GetRecordCount(pDelimPos)+1 = 1) then
        pDelimPos := pDelimPos || to_char(pLen+2) || ',';      
        return(pDelimPos);
      end if;
   
      /*  
      if(j>2 and ValueAt(pDelimPos,(j-1)) <> (i-1)) then
          pDelimPos := pDelimPos || (i-1) || ',';    
        return(pDelimPos);
      end if;
      */

      return(pDelimPos);

      exception 
        when others then        
          PrintVal('ERROR in GetDelimPos',1,1);
          raise;
          return(null);   
    end;   
  
  end GetDelimPos; 

  --function to get tuple of positions 1 and 2:
  function GetPos1Pos2(pDelimPos varchar2, pOccur int) return varchar2 as
  p1  int;
  p2  int;
  begin
    begin
      p1 := instr(pDelimPos,',',1,pOccur);
      p2 := instr(pDelimPos,',',1,(pOccur+2));
      exception 
        when others then        
          PrintVal('ERROR in GetPos1Pos2',1,1);
          raise;
          return(null);   
    end;
    return(substr(pDelimPos,p1,(p2-p1+1)));
  end GetPos1Pos2;

  --function to get first or second position from tuple:
  function GetPos(pTuple varchar2, pPosNum int) return int as
  p1 int;
  p2 int;
  begin
    begin
      if(pPosNum not in(1,2)) then return(NULL); end if;
        if(pPosNum=1) then
          p1 := instr(pTuple,',',1,1);
          p2 := instr(pTuple,',',1,2);    
      else 
        p1 := instr(pTuple,',',1,2);
        p2 := instr(pTuple,',',1,3);    
      end if;     
      exception 
        when others then        
          PrintVal('ERROR in GetPos',1,1);
          raise;
          return(null);   
     end;
     return(substr(pTuple,(p1+1),(p2-p1-1)));
  end GetPos;

  --function to remove leading "s" from value:
  function Des(pVal varchar2) return varchar2 is
  begin
    if(length(trim(pVal))=3 and ascii(substr(pVal,1,1))=0 and substr(pVal,2,1) = 's' and ascii(substr(pVal,3,1))=0) then
      return(null);
    end if;
    if(length(trim(pVal))=2 and ascii(substr(pVal,1,1))=0 and substr(pVal,2,1) = 's') then
      return(null);
    end if;
    if(ascii(substr(pVal,1,1))=0 and substr(pVal,2,1) = 's') then
      return(substr(pVal,3));
    end if;
    if(substr(pVal,1,1) = 's') then
      return(substr(pVal,2));
    else
      return pVal;
    end if;
  end;

  --function to get value at specified position in CLOB value (overloaded):
  function ValueAt(pVal clob, pDelimPos varchar2, i int) return varchar2 as
  pPos1Pos2 varchar2(32767);
  pPos1	 int;
  pPos2  int;
  begin
    begin
      pPos1Pos2 := GetPos1Pos2(pDelimPos,i);
      pPos1 := GetPos(pPos1Pos2,1);
      pPos2 := GetPos(pPos1Pos2,2);
      exception 
        when others then        
          PrintVal('ERROR in ValueAt (clob)',1,1);
          raise;
          return(null);   
    end;
    return(Des(substr(DelimWrap(pVal,','),pPos1+1,(pPos2-0-pPos1))));
  end ValueAt; 

  --procedure to get sample clob data:
  procedure GetClobSample1(pOwner varchar2, pTable varchar2) is
  pCursor   sys_refcursor;
  pClob     clob	 ;  
  pOBID     varchar2(24) ;
  begin
    begin
      pCursor := GetCursor1(pOwner, pTable, 'OBID', 'ZBLOB', 1);
      fetch pCursor into pOBID;   
      pClob := Long2Clob(pOwner, pOBID);
      if(pClob is null) then
        return;
      end if;
      PrintVal('pClob: ' || pClob,1,1);      
      exception 
        when others then        
          PrintVal('ERROR in GetClobSample1. OBID: ' || pOBID ,1,1);
          raise;
          return;   
    end;
  end GetClobSample1; 

  --procedure to get sample clob data (overloaded):
  procedure GetClobSample2(pOwner varchar2, pTable varchar2, pOBID varchar2) is
  pCursor   sys_refcursor;
  pClob     clob	 ;  
  pOBID1    varchar2(24) ;
  begin
    begin
      PrintVal('OBID: ' || pOBID);
      pCursor := GetCursor2(pOwner, pTable, pOBID);
      fetch pCursor into pOBID1;   
      pClob := Long2Clob(pOwner, pOBID1);
      if(pClob is null) then
        PrintVal('pClob IS NULL',1,1);
        return;
      end if;
      PrintVal('pClob: ' || pClob,1,1);      
      exception 
        when others then        
          PrintVal('ERROR in GetClobSample2. OBID: ' || pOBID ,1,1);
          raise;
          return;   
    end;
  end GetClobSample2; 

  --procedure to get ZBINDATA data for table:
  procedure CreateZBinData(pOwner varchar2, pTable varchar2, pProperties varchar2, pStageOwner varchar2, pSkip smallint default 0, pTotalSessions smallint, pSessionNum smallint) is
  pCursor   	sys_refcursor	;
  pOBID     	varchar2(24) 	;
  pTarget   	varchar2(255)	:= 'ZBINDB_' || pTable || '_' || replace(pProperties,',','_');
  pPropCount 	smallint 	:= (GetRecordCount(pProperties)+1);
  i		int		:= 1;
    --primary sub-procedure:
    procedure CreateZBinDataSub(pOBID varchar2) is
    type 	 StringArray is varray(10) of varchar2(32767);
    pDelimPoss   StringArray := StringArray('','','','','','','','','','');
    type	 ClobArray   is varray(10) of clob;
    pSubClobs	 ClobArray := ClobArray('','','','','','','','','','');
    pClob        clob;
    pPos1Pos2    varchar(25);
    pPos1        int ;
    pPos2        int ;
    pCount     	 int;   
    pDelimPos    varchar2(32767);
    pVal 	 clob;
    pValTemp     varchar2(32767);
    pProperty    varchar2(255);    
 
    --sub-procedure to get sub-clobs and delim positions for each property:  
    procedure GetSubClobsAndDelimPositions is
    begin
      begin 
	if(pClob is null) then
          PrintVal('ERROR in GetSubClobsAndDelimPositions (NULL pClob)'); 
	  return; 
        end if;
        for i in 1 .. (GetRecordCount(pProperties)+1) loop
          pSubClobs(i)  := GetZbinData(pClob,ValueAt(pProperties,i));         
	  if(pSubClobs(i) is null) then 
            PrintVal('ERROR in GetSubClobsAndDelimPositions (NULL pSubClob)'); 
	    return; 
          end if;

          pDelimPoss(i) := GetDelimPos(pSubClobs(i));   
          if(pDelimPoss(i) is null) then  
            return;
          end if;    
          if(ValueAt(pDelimPoss(i),1) <> '1') then 
	    pDelimPoss(i) := ',1' || pDelimPoss(i);
          end if; 
        end loop;  
	exception 
          when others then        
            PrintVal('ERROR in GetSubClobsAndDelimPositions',1,1);
            raise;
            return;   
      end;
    end GetSubClobsAndDelimPositions;

    --sub-procedure to insert records into staging table:
    procedure InsertRecords(pSkip smallint) is
    pStmt varchar2(32767);
    k 	  smallint;
    begin	
      begin
        --get count of values (that is, values in a sub-clob):
        pCount := (GetRecordCount(pDelimPoss(1)));
        if(pCount=0) then
          return;
        end if;    
        --for insert rows into staging table:
	k := 0;
        for i in (1+pSkip) .. pCount loop        
	  pStmt := 'insert /*+APPEND*/ into ' || pStageOwner || '.' || pTarget || '(OBID,ID,' || pProperties || ') values(:pOBID,:i,';
    	  for j in 1 .. pPropCount loop
            pValTemp := replace(nvl(trim(substr(ValueAt(pSubClobs(j),pDelimPoss(j),i),1,4000)),''),'''','"');
       	    if(j=1) then 
 	      pStmt := pStmt || quote(pValTemp);
 	    else
	      pStmt := pStmt || ',' || quote(pValTemp);
 	    end if;
          end loop;
          pStmt := pStmt || ')';     
          if(pPropCount=1 and pValTemp is null) then          
	    null;
          else
	    k:=(k+1);
  	    execute immediate pStmt using pOBID, (k);       
          end if;
        end loop;   
	exception 
          when others then        
            PrintVal('ERROR in InsertRecords',1,1);
            raise;
            return;   
      end;
    end InsertRecords;

    --"main" GetZBinDataSub:
    begin
      pClob := Long2Clob(pOwner, pOBID); 
      if(pClob is null) then 
        PrintVal('NULL pClob',1,1); 
        return; 
      end if;
      GetSubClobsAndDelimPositions();
      if(pDelimPoss(1) is null) then  
        return;
      end if;    
      InsertRecords(pSkip);   
      commit;   
      exception 
          when others then        
            PrintVal('ERROR in CreateZBinDataSub. OBID: ' || pOBID,1,1);
            raise;
            return;   
    end CreateZBinDataSub;
begin
  begin
    pTarget := 'ZBINDB_' || pTable || '_' || ValueAt(pProperties,1);
    if((GetRecordCount(pProperties)+1) > 1) then
      pTarget := pTarget || '_ETC';
    end if;
    MakeTable(pStageOwner,pTarget,pProperties); 
    if((GetRecordCount(pProperties)+1) = 1) then
      pCursor := GetCursor1(pOwner, pTable, 'OBID', ValueAt(pProperties,1));       
    else
      pCursor := GetCursor1(pOwner, pTable, 'OBID', 'ZBLOB'); 
    end if;
    loop 
      fetch pCursor into pOBID;  
      if(i=1 and pOBID is null) then
        PrintVal('no records found in source table');
        exit;
      end if;
      exit when pCursor%NOTFOUND;
      if((mod(i,pTotalSessions)+1) = pSessionNum) then
       CreateZBINDATASub(pOBID);  
      end if;
  --    exit; 
      i:=(i+1);    
    end loop;   
    PrintVal('Table ' || pStageOwner || '.' || pTarget || ' created and populated.',pPrintNum,1.0);
    exception 
      when others then        
        PrintVal('ERROR in CreateZBinData. OBID: ' || pOBID,1,1);
        raise;
        return;   
    end;
end CreateZBinData;

--main:
begin        

--GetClobSample1('ACTIVE', 'SAVCOLP');
--GetClobSample2('ACTIVE', 'J0URLITM', 'AbeuyXatce-cractive--Zs6');

  CreateZBinData(
		 'infodba', 	 		--pOwner
		 'ASSEMBLY', 	 		--pTable
		 'B2OBJPARTICIPANTLIST',	--pProperty
 		 'infodba',	   		--pStageOwner
		  0,		 		--pSkip
                  1,				--pTotalSessions
                  1				--pSessionNum
		);


end;
/


exit



