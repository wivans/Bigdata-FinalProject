Final Project : Clustering System REST-API with data streams

Struktur sistem :

Server.py
App.py
Engine.py
Producer.py
Consumer.py

Model yang digunakan :

Model 1 : 1/2 data pertama
Model 2: 1/2 data pertama + 1/2 data kedua (semua data)

Batas jumlah data yang diterima :

Batas jumlah data yang diterima adalah 1000 data per detik

Cara Menjalankan :

1. Nyalakan Zookeeper
2. Nyalakan kafka
3. Buatlah topik baru di kafka
4. Jalankan Producer.py
5. Jalankan Consumer.py
6. Jalankan server.py

URL yang dapat diakses :
http://localhost:5432/AppleStore/(nomor model)
