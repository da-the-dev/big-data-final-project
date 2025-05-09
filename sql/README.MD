# SQL sripts

<table>
    <tr>
        <th>File</th>
        <th>Description</th>
    </tr>
    <tr>
        <td><code><a href='create_tables.sql'>create_tables.sql</a></code></td>
        <td>Create Postgres tables for raw data</td>
    </tr>
    <tr>
        <td><code><a href='import_data.sql'>import_data.sql</a></code></td>
        <td>Read and import all of the data from csv into Postgres</td>
    </tr>
    <tr>
        <td><code><a href='test_database.sql'>test_database.sql</a></code></td>
        <td>Test Postgres DB that data was imported into the correct table</td>
    </tr>
    <tr>
        <td><code><a href='db.hql'>db.hql</a></code></td>
        <td>Create Hive data for tables from .parquet file</td>
    </tr>
    <tr>
        <td><code><a href='load_state.hql'>load_state.hql</a></code></td>
        <td>Load data into state partitions state by state (to not crash the cluster)</td>
    </tr>
    <tr>
        <td><code><a href='create_ml_tables.hq'>create_ml_tables.hql</a></code></td>
        <td>Creates Hive tabels for ML model predictions</td>
    </tr>
    <tr>
        <td><code><a href='cleanup.hql'>cleanup.hql</a></code></td>
        <td>Cleans up Hive br for reproducibility</td>
    </tr>
    <tr>
        <td><code><a href='dashboard/'>dashboard/</a></code></td>
        <td>Scripts for EDA</td>
    </tr>
</table>