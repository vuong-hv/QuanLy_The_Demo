# Chay tren iPhone bang IP public

Ung dung hien tai la Flask + SQLite, khong phai app iOS native.
Huong dung phu hop voi code hien tai la chay backend tren server co IP public, sau do mo bang Safari tren iPhone va them vao man hinh chinh de dung nhu mot app.

## 1. Cai thu vien

```bash
pip install -r requirements.txt
```

## 2. Chay app cho iPhone truy cap

```bash
PUBLIC_IP=YOUR_PUBLIC_IP APP_HOST=0.0.0.0 APP_PORT=5001 APP_DEBUG=false python run_ios.py
```

Vi du:

```bash
PUBLIC_IP=113.161.10.20 APP_HOST=0.0.0.0 APP_PORT=5001 APP_DEBUG=false python run_ios.py
```

## 3. Mo tren iPhone

Mo Safari va vao:

```text
http://YOUR_PUBLIC_IP:5001
```

Neu muon icon tren man hinh chinh mo thang trang chi tiet giao dich, vao truc tiep:

```text
http://YOUR_PUBLIC_IP:5001/all_transactions
```

Neu dung domain, mo:

```text
http://your-domain.com:5001
```

## 4. Cai len iPhone nhu mot app

1. Mo link bang Safari tren iPhone.
2. Bam nut `Share`.
3. Chon `Add to Home Screen`.
4. Dat ten ung dung, vi du `QuanLyNH`.
5. Bam `Add`.

Sau buoc nay iPhone se hien icon app tren man hinh chinh va mo dung trang anh da mo truoc khi bam `Add to Home Screen`.

Neu truoc day da them icon cu roi, hay xoa icon cu tren iPhone va them lai de tranh dung manifest da cache.

## 5. Database

Mac dinh app dung file SQLite:

```text
data_nganhang.db
```

Neu muon doi file database:

```bash
DB_FILE=/duong_dan/data_nganhang.db PUBLIC_IP=YOUR_PUBLIC_IP python run_ios.py
```

## 6. Luu y quan trong

- Khong nen de iPhone ket noi truc tiep vao file SQLite qua IP public.
- Huong dung dung la iPhone ket noi vao web app Flask; Flask moi doc/ghi database.
- Can mo firewall/NAT cho port `5001` neu muon truy cap tu internet.
- De an toan hon, nen dat Nginx + HTTPS o phia truoc Flask neu dua vao su dung that.
- Neu anh can file `.ipa` de cai bang Xcode/TestFlight, can lam them mot project iOS native hoac WebView wrapper rieng.
