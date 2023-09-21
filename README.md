# sql Docker


### Variables
```bash

PASSWD="SQDev25";
```

```bash
function mysqlStart {
  docker run --name mysql -p 3306:3306 -v ${HOME}/Documents/mysqldb/:/var/lib/mysql -e MYSQL_DATABASE=TestDB -e MYSQL_ROOT_PASSWORD=${PASSWD} -e MYSQL_USER=admin -e MYSQL_PASSWORD=${PASSWD} -itd mysql:5.7;
}
mysql -u admin -p"${PASSWD}" -h 127.0.0.1 -D TestDB;
# detener
docker stop $(docker ps -qa)
```

### Create DB Table

- Inser sql table test:
```sql
CREATE DATABASE TestDB;

USE TestDB;

CREATE TABLE TestDB.Users (
  id SERIAL PRIMARY KEY,
  identification INT NOT NULL UNIQUE,
  name VARCHAR(128),
  email VARCHAR(64),
  age INT CHECK(edad >= 0 AND edad <= 128)
);

-- ALTER TABLE TestDB.Users
-- MODIFY COLUMN identification INT NOT NULL;

INSERT INTO TestDB.Users VALUES (null, 1016995859, 'user name-1', 'name-1@mail.com', 38);
INSERT INTO TestDB.Users VALUES (null, 1016995860, 'user name-2', 'name-2@mail.com', 35);

SELECT * FROM TestDB.Users WHERE age > 36;
```

### Inserts For
- From bash
```bash
for i in {1..100};do
  mysql -u root -p"${PASSWD}" -h 127.0.0.1 -D TestDB -e "INSERT INTO TestDB.Users VALUES (null, 1016${i}, 'user name-${i}', 'name-${i}@mail.com', ${i})";
done;
```

## Create bigquery table

- Variables
```bash
export PROJECT_ID="poxs4-datalake-prd";
export DATASET_MAIN="dataset_test_persons_01";
export TABLE_MAIN="table_user";
export TABLE_MAIN_SCHEMA="$(pwd)/schema_users.json";
```
- Create dataset and table
```bash
bq mk --dataset "${PROJECT_ID}:${DATASET_MAIN}";
bq mk --table --schema "${TABLE_MAIN_SCHEMA}" "${PROJECT_ID}:${DATASET_MAIN}.${TABLE_MAIN}";
```