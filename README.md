# Final Project : Clustering System REST-API with data streams

## Struktur sistem :

### Server.py
Inisialisasi CherryPy web server dan untuk tempat inisialisasi fungsi dan memanggil app.py

### App.py
menghubungkan server.py dengan engine.py dan sebagai tempat routing

### Engine.py
Engine.py melakukan pembacaan dataset, membuat 2 model yang berbeda

### Producer.py
berperan sebagai producer pada kafka sebagai pengirim data ke kafka server dengan delay yang sudah didetapkan

### Consumer.py
berperan sebagai konsumer sebagai pengambil data dari kafka server dan menyimpan data ke dalam format .txt

## Model yang digunakan :

1. Model 1 : 1/2 data pertama
2. Model 2: 1/2 data pertama + 1/2 data kedua (semua data)

## Batas jumlah data yang diterima :

Batas jumlah data yang diterima adalah 1000 data per detik

## Cara Menjalankan :

1. Nyalakan Zookeeper
2. Nyalakan kafka
3. Buatlah topik baru di kafka, nama topic dalam project saya adalah Applestore

``` kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Applestore ```

4. Jalankan Producer.py
5. Jalankan Consumer.py
6. Jalankan server.py

#### URL yang dapat diakses :
``` http://localhost:5432/AppleStore/(nomor model) ```
#### Untuk menampilkan cluster terdekat
![cluster](img/FP.png)
