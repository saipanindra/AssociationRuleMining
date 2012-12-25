CREATE OR REPLACE PROCEDURE GenerateFI (s IN INTEGER, p IN NUMBER)
AS
   CURSOR c1
   IS
        SELECT temp.itemid,temp.counttrans*100/temp2.uniquetrans as percent
          FROM (  SELECT t.itemid, i.itemname iname, COUNT (t.transid) counttrans
                    FROM trans t, items i
                   WHERE i.itemid = t.itemid
                GROUP BY i.itemname, t.itemid
                  HAVING COUNT (t.transid) >=
                            (SELECT COUNT (DISTINCT transid) * p FROM trans)) temp,
               (SELECT COUNT (DISTINCT transid) uniquetrans FROM trans) temp2
      ORDER BY ROUND ( (temp.counttrans / temp2.uniquetrans) * 100, 2);

   CURSOR c2
   IS
      SELECT DISTINCT IS1.ISetID ID1, IS2.ISetID ID2
        FROM FISet IS1, FISet IS2
       WHERE     IS1.ISetID > IS2.ISetID
             AND s =
                    (SELECT COUNT (DISTINCT ItemID)
                       FROM FISet IS3
                      WHERE    IS1.ISetID = IS3.ISetID
                            OR IS3.ISetID = IS2.ISetID);


   fisentry              c1%ROWTYPE;
   iden                INTEGER := 1;
   prs                 c2%ROWTYPE;
   subcount            INTEGER;
   freqcount           INTEGER;
   transpercentcount   NUMBER;
   iter                INTEGER := 1;
   duplicatecount INTEGER;
BEGIN
   DBMS_OUTPUT.ENABLE (1000000);
   DBMS_OUTPUT.put_Line ('S=' || s || ' P=' || p);
   DBMS_OUTPUT.PUT_LINE ('HELLO');

   IF s = 1
   THEN
      DELETE FROM FISet;

      OPEN c1;

      LOOP
         FETCH c1 INTO fisentry;

         EXIT WHEN c1%NOTFOUND;

         INSERT INTO FISet (isetid, itemid,percent)
              VALUES (iden, fisentry.itemid,fisentry.percent);

         iden := iden + 1;
      END LOOP;

      CLOSE c1;
   /*EXECUTE IMMEDIATE 'CREATE TABLE temp ( itemid INTEGER)';*/

   ELSE
      DBMS_OUTPUT.put_line ('In the Else part');
      GenerateFI (s - 1, p);

      DELETE FROM pairs;



      OPEN c2;

      /*EXECUTE IMMEDIATE 'DROP TABLE TempFISet';*/

      EXECUTE IMMEDIATE 'INSERT INTO TempFISet (Select * from FISet)';

      DELETE FROM FISet;

      iter := 1;

      LOOP
         FETCH c2 INTO prs;

         EXIT WHEN c2%NOTFOUND;

         DELETE FROM temp;

         DBMS_OUTPUT.put_Line (
            'Pairs.id1:' || prs.id1 || 'Pairs.id2:' || prs.id2);

         EXECUTE IMMEDIATE
               'INSERT INTO temp(Select Distinct itemid from TempFISet s Where s.IsetID = '
            || prs.id1
            || ' or s.Isetid = '
            || prs.id2
            || ')';
         SELECT COUNT(*) INTO duplicatecount FROM(
		 SELECT  count(*)  FROM fiset f, temp t WHERE f.itemid = t.itemid GROUP BY isetid
	     HAVING count(*) = s);
		IF duplicatecount = 0 THEN
         SELECT COUNT (*)
           INTO subcount
           FROM ( (  SELECT DISTINCT TempFISet.ISetID
                       FROM TEMP, TempFISet
                      WHERE TEMP.ItemID = TempFISet.ItemID
                   GROUP BY TempFISet.ISetID
                     HAVING COUNT (*) = s - 1));

         /* DBMS_OUTPUT.put_LIne('SUBCOUNT '||subcount);*/

         IF subcount = s
         THEN
            SELECT COUNT (*)
              INTO freqcount
              FROM (  SELECT COUNT (transid)
                        FROM trans tn, temp te
                       WHERE tn.ITEMID = te.ITEMID
                    GROUP BY tn.transid
                      HAVING COUNT (*) >= s);

            SELECT COUNT (DISTINCT transid) * p
              INTO transpercentcount
              FROM trans;

            IF freqcount >= transpercentcount
            THEN
               INSERT INTO FISet (Itemid)
                  SELECT itemid FROM temp;

               EXECUTE IMMEDIATE
                     'UPDATE FISet Set ISetid = '
                  || iter
                  || ' WHERE isetid IS null AND itemid IS NOT null';

               EXECUTE IMMEDIATE
                     'UPDATE FISet Set percent = '
                  ||  freqcount * p * 100 / transpercentcount
                  || ' WHERE isetid IS not null AND itemid IS NOT null AND percent is null';

               iter := iter + 1;
			  END IF;
               DELETE FROM TEMP;
            END IF;
         END IF;
      END LOOP;

      CLOSE c2;
   END IF;

   DELETE FROM temp;

   DELETE FROM tempFISet;

   DELETE FROM pairs;
END;
/
CREATE OR REPLACE PROCEDURE GenerateAR (c   IN NUMBER,
                                                 s   IN INTEGER,
                                                 p   IN NUMBER)
AS
   CURSOR c2
   IS
      SELECT DISTINCT IS1.ISetID ID1, IS2.ISetID ID2
        FROM FISet IS1, FISet IS2
       WHERE     IS1.ISetID > IS2.ISetID
             AND s =
                    (SELECT COUNT (DISTINCT ItemID)
                       FROM FISet IS3
                      WHERE    IS1.ISetID = IS3.ISetID
                            OR IS3.ISetID = IS2.ISetID);

   CURSOR tempcursor
   IS
      SELECT itemid FROM tempset;



   iden                INTEGER := 1;
   prs                 c2%ROWTYPE;
   subcount            INTEGER;
   freqcount           INTEGER;
   transpercentcount   NUMBER;
   iter                INTEGER := 1;
   duplicatecount      INTEGER;
   totalsupport        NUMBER;
   leftsupport         NUMBER;
   set_support         NUMBER;
   counter             NUMBER;

   TYPE setTable IS TABLE OF temp.itemid%TYPE
      INDEX BY BINARY_INTEGER;

   settab              setTable;
   binary_indicator    INTEGER;
   set_index           INTEGER;
   right_count         INTEGER;
   left_count          INTEGER;
BEGIN
   DBMS_OUTPUT.ENABLE (1000000);
   DBMS_OUTPUT.put_Line ('S=' || s || ' P=' || p);
   DBMS_OUTPUT.PUT_LINE ('HELLO');

   DBMS_OUTPUT.put_line ('In the Else part');
   GenerateFI (s - 1, p);

   DELETE FROM pairs;

   DELETE FROM artable;


   OPEN c2;

   --looping through sets of items of size s
   LOOP
      FETCH c2 INTO prs;

      EXIT WHEN c2%NOTFOUND;

      DELETE FROM temp;

      DBMS_OUTPUT.put_Line (
         'Pairs.id1:' || prs.id1 || 'Pairs.id2:' || prs.id2);

      --Check if the set has support >= min support
      EXECUTE IMMEDIATE
            'INSERT INTO temp(Select Distinct itemid from FISet s Where s.IsetID = '
         || prs.id1
         || ' or s.Isetid = '
         || prs.id2
         || ')';

      --check for duplicates
      SELECT COUNT (*)
        INTO duplicatecount
        FROM (  SELECT COUNT (*)
                  FROM ARTABLE a, temp t
                 WHERE a.itemid = t.itemid
              GROUP BY ruleid
                HAVING COUNT (*) = s);

      IF duplicatecount = 0
      THEN
         --If not duplicate, check for support
         SELECT getsupport INTO set_support FROM DUAL;

         DBMS_OUTPUT.put_line (
            'set_support:' || set_support || 'p*100' || p * 100);

         IF set_support >= p * 100
         THEN
            --Get each set into tempset
            DELETE FROM tempset;

            EXECUTE IMMEDIATE
                  'INSERT INTO tempset(Select Distinct itemid from FISet s Where s.IsetID = '
               || prs.id1
               || ' or s.Isetid = '
               || prs.id2
               || ')';

            --store set in table array
            counter := 1;

            FOR tc IN tempcursor
            LOOP
               settab (counter) := tc.itemid;
               counter := counter + 1;
            END LOOP;

            DBMS_OUTPUT.put_line ('COUNTER IS ' || counter);

            --iterating from 1 to 2^(s)-2 to generate all possible subsets based on binary representation of each of the numbers
            FOR i IN 1 .. POWER (2, s) - 2
            LOOP
               binary_indicator := i;
               set_index := s;

               DELETE FROM tempright;

               FOR k IN 1 .. s
               LOOP
                  DBMS_OUTPUT.put_line (
                     'BINARY INDICATOR VALUE' || binary_indicator);

                  IF MOD (binary_indicator, 2) = 1
                  THEN
                     INSERT INTO tempright
                          VALUES (settab (set_index));
                  END IF;

                  binary_indicator := FLOOR (binary_indicator / 2);
                  set_index := set_index - 1;
               END LOOP;

               IF (i = 3)
               THEN
                  SELECT COUNT (*) INTO right_count FROM tempright;

                  SELECT COUNT (*)
                    INTO left_count
                    FROM tempset
                   WHERE itemid NOT IN (SELECT itemid FROM tempright);

                  DBMS_OUTPUT.put_LINE ('RIGHT SET COUNT' || right_count);
                  DBMS_OUTPUT.put_Line ('LEFT SET COUNT' || left_count);
               END IF;

               --Get Total support of set;
               DELETE FROM temp;

               INSERT INTO temp
                  (SELECT itemid FROM tempset);

               SELECT getsupport INTO totalsupport FROM DUAL;

               --Get support of right set;
               DELETE FROM temp;

               INSERT INTO temp
                  (SELECT itemid
                     FROM tempset
                    WHERE itemid NOT IN (SELECT itemid FROM tempright));

               SELECT getsupport INTO leftsupport FROM DUAL;

               DBMS_OUTPUT.put_line (
                     'Total support'
                  || totalsupport
                  || ' Left support:'
                  || leftsupport);

               --If support of set /support of rightset >= confidence, store the associatioon rule (set-rightset)->rigtset into association rule table
               IF (totalsupport / leftsupport >= c)
               THEN
                  SELECT COUNT (DISTINCT ruleid) + 1 INTO iter FROM artable;

                  --Insert items in leftset into artable
                  EXECUTE IMMEDIATE
                        'insert into ARTABLE(select '
                     || iter
                     || ',t.itemid,i.itemname,'
                     || totalsupport / leftsupport * 100
                     || ',''Y'','
                     || totalsupport
                     || ' from tempset t,items i where t.itemid = i.itemid
		    and t.itemid not in (select itemid from tempright))';

                  --Insert items in rightset into artable
                  EXECUTE IMMEDIATE
                        'insert into ARTABLE(select '
                     || iter
                     || ',t.itemid,i.itemname,'
                     || totalsupport / leftsupport * 100
                     || ','' '','
                     || totalsupport
                     || ' from tempright t,items i where t.itemid = i.itemid)';
               END IF;
            END LOOP;
         END IF;
      END IF;
   END LOOP;

   CLOSE c2;


   DELETE FROM temp;

   DELETE FROM tempFISet;

   DELETE FROM pairs;
END;
/

CREATE OR REPLACE FUNCTION getSupport
   RETURN NUMBER
IS
   counttrans      INTEGER;
   distincttrans   INTEGER;
BEGIN
   SELECT COUNT (*)
     INTO counttrans
     FROM (  SELECT tr.transid, COUNT (tr.transid) reqtrans
               FROM trans tr, temp t
              WHERE tr.itemid = t.itemid
           GROUP BY tr.transid
             HAVING COUNT (*) = (SELECT COUNT (*) FROM temp));

   SELECT COUNT (DISTINCT transid) INTO distincttrans FROM trans;

   RETURN counttrans / distincttrans * 100;
END;
/
exit;

