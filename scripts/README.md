# Scripts

<table>
    <tr>
        <th>Path</th>
        <th>Description</th>
    </tr>
    <tr>
        <td><a href='build_projectdb.py'><code>build_projectdb.py</code></a></td>
        <td>Inserts raw csv data into Postgres by running 
        <code><a href="../sql/create_tables.sql">sql/create_tables.sql</a></code>, 
        <code><a href="../sql/import_data.sql">sql/import_data.sql</a></code>, 
        <code><a href="../sql/test_database.sql">sql/test_database.sql</a></code>.</td>
    </tr>
    <tr>
        <td><code><a href='setup_python.sh'>setup_python.sh</a></code></td>
        <td>Downloads python deps and activates fresh venv.</td>
    </tr>
    <tr>
        <td><code><a href='data_collection.sh'>data_collection.sh</a></code></td>
        <td>Downloads raw csv data from Kaggle and unpacks it into <code><a href="../data">data/</a></code>.</td>
    </tr>
    <tr>
        <td><code><a href='data_storage.sh'>data_storage.sh</a></code></td>
        <td>Runs <code><a href="build_projectdb.py">scripts/build_projectdb.py</a></code>.</td>
    </tr>
    <tr>
        <td><code><a href='data_ingestion.sh'>data_ingestion.sh</a></code></td>
        <td>Injects data from Postgres into Snappy-compressed <code>.parquet</code> file on HDFS.</td>
    </tr>
    <tr>
        <td><code><a href='prepare_data.py'>prepare_data.py</a></code></td>
        <td>Part of stage 3, does data modeling.</td>
    </tr>
    <tr>
        <td><code><a href='prepare_dashboard.sh'>prepare_dashboard.sh</a></code></td>
        <td>Runs all of <code>sql/q*.hql</code> queries on Hive, saves the results to <code>output/</code>.</td>
    </tr>
    <tr>
        <td><code><a href='train_models.py'>train_models.py</a></code></td>
        <td>Preprocesses the data for ML models, sets up grid search for hyperparameters, and then does model training.</td>
    </tr>
    <tr>
        <td><code><a href='preprocess.sh'>preprocess.sh</a></code></td>
        <td>Runs <code><a href="setup_python.sh">scripts/setup_python.sh</a></code> and <code><a href="data_collection.sh">scripts/data_collection.sh</a></code></td>
    </tr>
    <tr>
        <td><code><a href='stage1.sh'>stage1.sh</a></code></td>
        <td>Stage 1 - Postgres + Scoop</td>
    </tr>
    <tr>
        <td><code><a href='stage2.sh'>stage2.sh</a></code></td>
        <td>Stage 2 - Hive db setup + partitioned loading for data + EDA</td>
    </tr>
    <tr>
        <td><code><a href='stage3.sh'>stage3.sh</a></code></td>
        <td>Stage 3 - Data preparation + ML model training</td>
    </tr>
    <tr>
        <td><code><a href='stage4.sh'>stage4.sh</a></code></td>
        <td>Stage 4 - Saves ML results into a db on Hive</td>
    </tr>
    <tr>
        <td><code><a href='postprocess.sh'>postprocess.sh</a></code></td>
        <td>Unused</td>
    </tr>
</table>