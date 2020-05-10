#Usage
#Copy this to main.sh
#Add execute permissions using chmod +x main.sh
#Create 2 files table1 and tables2 using contents from files tables1 and tables2 after this
#Run using ./main.sh tables1 and ./main.sh tables2 to run 2 jobs in parallel
CMD="sqoop import --connect \"jdbc:mysql://[HOSTNAME]/[DBNAME]\" --username [USERNAME] --password [PASSWORD] --table [TABLENAME] --num-mappers [MAPPERS] --target-dir /user/rposam2020/livedemo/[TABLENAME]"

STARTDATE=`date +'%Y%m%d'`
HOSTNAME=`grep HOSTNAME db.properties|cut -d= -f2`
DBNAME=`grep DATABASE db.properties|cut -d= -f2`
USERNAME=`grep USERNAME db.properties|cut -d= -f2`
PASSWORD=`grep PASSWORD db.properties|cut -d= -f2`

for LINE in `cat tables`;
do
  TABLE=`echo ${LINE}|cut -d: -f1`
  MAPPERS=`echo ${LINE}|cut -d: -f2`
  SPLITBY=`echo ${LINE}|cut -d: -f3`
  SQOOPCMD=`echo $CMD|sed -e "s/\[TABLENAME\]/${TABLE}/g"`
  SQOOPCMD=`echo $SQOOPCMD|sed -e "s/\[MAPPERS\]/${MAPPERS}/g"`
  SQOOPCMD=`echo $SQOOPCMD|sed -e "s/\[HOSTNAME\]/${HOSTNAME}/g"`
  SQOOPCMD=`echo $SQOOPCMD|sed -e "s/\[DBNAME\]/${DBNAME}/g"`
  SQOOPCMD=`echo $SQOOPCMD|sed -e "s/\[USERNAME\]/${USERNAME}/g"`
  SQOOPCMD=`echo $SQOOPCMD|sed -e "s/\[PASSWORD\]/${PASSWORD}/g"`
  if [ "${SPLITBY}" != "NA" ]; then
    SQOOPCMD="${SQOOPCMD} --split-by ${SPLITBY}"
  else
    SQOOPCMD="${SQOOPCMD} --autoreset-to-one-mapper"
  fi
  echo ${SQOOPCMD} >> sqoop_gen_script.log
  echo "Loading ${TABLENAME} using ${SQOOPCMD}"
  `${SQOOPCMD} 2>sqoop_${STARTDATE}_${TABLE}.err 1>sqoop_${STARTDATE}_${TABLE}.out`
  NOOFRECORDS=`grep "Retrieved .* records" sqoop_${STARTDATE}_${TABLE}.err|cut -d" " -f6`
  echo ${STARTDATE}:${TABLE}:${NOOFRECORDS} >> sqoop_livedemo.log
done