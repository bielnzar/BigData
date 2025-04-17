# Latihan Hadoop, Pig & Hive

Mata Kuliah: Big Data dan Data Lakehouse (A)

Dosen Pengampu: Fuad Dary Rosyadi, S.Kom., M.Kom.

| Nama | NRP |
| :--------: | :-------: |
| Nabiel Nizar Anwari | 5027231087 |

**Excercise Hadoop, PIG, and Hive**
[menggunakan dataset movielens 100k](https://grouplens.org/datasets/movielens/)

Instalasi Hadoop + Tools [Disini](https://hub.docker.com/r/silicoflare/hadoop)


## Soal 1: Apache Hadoop (HDFS)

**Prerequisite**
```bash
wget https://files.grouplens.org/datasets/movielens/ml-100k.zip
unzip ml-100k.zip
```

1. Buat direktori movielens di HDFS.
```bash
hdfs dfs -mkdir /movielens
```
2. Upload file u.data ke direktori tersebut.
```bash
hdfs dfs -put /root/ml-100k/u.data /movielens/
```
3. Tampilkan 10 baris pertama dari file.
```bash
hdfs dfs -cat /movielens/u.data | head -n 10
```
4. Hitung ukuran file di HDFS.
```bash
hdfs dfs -ls -h /movielens/u.data
```


![image1](https://github.com/bielnzar/BigData/blob/main/Hadoop/Tugas1/HDFS/1.png)
![image2](https://github.com/bielnzar/BigData/blob/main/Hadoop/Tugas1/HDFS/2.png)
![image3](https://github.com/bielnzar/BigData/blob/main/Hadoop/Tugas1/HDFS/3.png)


## Soal 2: Apache Pig

1. Load file u.data ke Pig.
```bash
pig

raw_data = LOAD '/movielens/u.data' USING PigStorage('\t')
           AS (user_id:int, item_id:int, rating:int, timestamp:long);
```

2. Hitung rata-rata rating per item_id (film).
```bash
grouped = GROUP raw_data BY item_id;

avg_rating = FOREACH grouped GENERATE
             group AS item_id,
             AVG(raw_data.rating) AS avg_rating;
```

3. Ambil hanya film yang memiliki rating rata-rata ≥ 4.0.
```bash
fav_movies = FILTER avg_rating BY avg_rating >= 4.0;
```

4. Simpan hasil akhir ke output/film_favorit.
```bash
STORE fav_movies INTO '/output/film_favorit' USING PigStorage('\t');
```

Menampilkan Hasil:
```bash
quit

hdfs dfs -cat /output/film_favorit/part-*
```

![image1](https://github.com/bielnzar/BigData/blob/main/Hadoop/Tugas1/PIG/1.png)
![image2](https://github.com/bielnzar/BigData/blob/main/Hadoop/Tugas1/PIG/2.png)


## Soal 3: Apache Hive

1. Buat database movielens.
```bash
hive

CREATE DATABASE movielens;
USE movielens;
```

2. Buat tabel ratings (user_id INT, item_id INT, rating INT, timestamp BIGINT).
```bash
CREATE TABLE ratings (
  `user_id` INT,
  `item_id` INT,
  `rating` INT,
  `timestamp` BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';
```

3. Load data dari file u.data.
```bash
LOAD DATA INPATH '/movielens/u.data' INTO TABLE ratings;
```

4. Hitung rata-rata rating setiap film.
```bash
SELECT item_id, AVG(rating) AS avg_rating
FROM ratings
GROUP BY item_id;
```

5. Ambil 10 film dengan rata-rata rating tertinggi.
```bash
SELECT item_id, AVG(rating) AS avg_rating
FROM ratings
GROUP BY item_id
ORDER BY avg_rating DESC
LIMIT 10;
```

![image1](https://github.com/bielnzar/BigData/blob/main/Hadoop/Tugas1/HIVE/1.png)
![image 2](https://github.com/bielnzar/BigData/blob/main/Hadoop/Tugas1/HIVE/2.png)
![image 3](https://github.com/bielnzar/BigData/blob/main/Hadoop/Tugas1/HIVE/3.png)
![image 4](https://github.com/bielnzar/BigData/blob/main/Hadoop/Tugas1/HIVE/4.png)
![image 5](https://github.com/bielnzar/BigData/blob/main/Hadoop/Tugas1/HIVE/5.png)

> Hamdalah