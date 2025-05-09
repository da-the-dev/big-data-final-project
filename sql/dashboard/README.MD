# EDA queries
Each script corresponds to a specific analytical query (e.g., severity vs. delay, top congested cities, weather impact, etc.) and was  used  to  generate  intermediate  datasets  as  input  for  dashboard  charts. 

Each query has its `.hql` file and the `.sh` file that runs that query. `.hql` queries are picked up and exectuted by <code><a href="../../scripts/prepare_dashboard.sh">scripts/prepare_dashboard.sh</a></code>. 

> [!IMPORTANT]
> `sql/q*.sh` are used to export the results to `output/` and were used for debugging.
