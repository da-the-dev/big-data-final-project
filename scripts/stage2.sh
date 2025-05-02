#!/bin/bash
password=$(head -n 1 secrets/.hive.pass)

# Initialize database
echo "Initializing database..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
   -n team26 -p "$password" \
   -f sql/db.hql \
   > output/db_init.log 2> output/db_init.err

# Get list of states
echo "Retrieving state list..."
states=$(beeline --silent=true \
   -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
   -n team26 -p "$password" \
   --outputformat=csv2 \
   -e "USE team26_projectdb; SELECT state FROM states_list;" \
   | grep -v "^state$" | tr -d '\r')

# Process each state
for state in $states; do
   echo "Processing state: $state"
   
   beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
      -n team26 -p "$password" \
      --hivevar STATE="$state" \
      -f sql/load_state.hql \
      > "output/${state}_load.log" 2> "output/${state}_load.err"
      
   if [ $? -ne 0 ]; then
      echo "Error processing $state"
      exit 1
   fi
done

# Final cleanup
echo "Performing final cleanup..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
   -n team26 -p "$password" \
   -f sql/cleanup.hql \
   > output/cleanup.log 2> output/cleanup.err

echo "Running analytical queries..."
for i in {1..4}; do
   beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 \
      -n team26 -p "$password" \
      -f "sql/q$i.hql" \
      > "output/q$i.log" 2> "output/q$i.err"
done

echo "Data processing completed successfully"
