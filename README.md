# Burung Internal Notificationing App

Layanan notifikasi realtime internal — menerima event perubahan data dari background worker via HTTP, menjalin koneksi WebSocket dengan klien (pengguna, kurir, seller), dan mengarsipkan setiap hasil notifikasi ke database.

## Stack

<div align="center">
  <table>
    <tr>
      <td align="center" width="90">
        <img src="https://go.dev/blog/go-brand/Go-Logo/PNG/Go-Logo_LightBlue.png" height="40" alt="go logo" />
        <br/><sub><b>Go</b></sub>
      </td>
      <td align="center" width="90">
        <img src="https://cdn.simpleicons.org/redis/FF4438" height="40" alt="redis logo" />
        <br/><sub><b>Redis</b></sub>
      </td>
      <td align="center" width="90">
        <img src="https://cdn.simpleicons.org/apachecassandra/1287B1" height="40" alt="cassandra logo" />
        <br/><sub><b>Cassandra</b></sub>
      </td>
    </tr>
  </table>
</div>

## Architecture

<img width="2720" height="3040" alt="burung_notificationing_app_architecture" src="https://github.com/user-attachments/assets/c5421d8c-edad-4918-96e9-277bf8158168" />


| Layer | Komponen | Peran |
|---|---|---|
| HTTP Trigger | burung-background-worker-app | Kirim event perubahan SOT via HTTP |
| App | burung-internal-notificationing-app | Receive, validasi, route, dispatch notifikasi |
| Session cache | Redis | Lookup session aktif user / kurir / seller |
| WebSocket | dispatcher internal | Maintain koneksi & push notifikasi realtime |
| Klien | Frontend user · Kurir · Seller | Terima notifikasi via WebSocket handshake |
| Message archive | Cassandra `message_archive_db` | Catat hasil notifikasi (sukses maupun gagal) |

## Flow

```
burung-background-worker-app
        │
        ▼ HTTP POST · PATCH · PUT · DELETE
burung-internal-notificationing-app
        │
        ├── Redis (session lookup)
        │       └── resolve user/kurir/seller session aktif
        │
        └── WebSocket dispatcher
                │
                ├── Frontend user  (browser / mobile app)
                ├── Kurir          (driver mobile app)
                └── Seller         (seller dashboard)
                        │
                        ▼ notification result
        Cassandra message_archive_db
                │  ├── write on SUCCESS
                │  └── write on FAILURE
                ▼
             Selesai ✓
```

## Cara Kerja

### 1. Menerima Event dari Background Worker
Background worker mengirim HTTP request (`POST`, `PATCH`, `PUT`, atau `DELETE`) yang berisi payload perubahan data SOT. Notificationing app menerima dan memvalidasi request ini sebelum meneruskannya.

### 2. Session Lookup via Redis
Sebelum mengirim notifikasi, app melakukan lookup ke Redis untuk memastikan siapa saja yang memiliki sesi aktif dan berhak menerima notifikasi tersebut — bisa user, kurir, atau seller.

### 3. Handshake & Push WebSocket
App mempertahankan koneksi WebSocket persisten dengan ketiga jenis klien. Begitu target sesi diketahui, notifikasi langsung di-push secara realtime melalui koneksi yang sudah established tanpa perlu polling.

### 4. Arsip ke Cassandra
Setelah operasi notifikasi selesai — baik berhasil diterima maupun gagal (misalnya klien disconnect) — hasilnya ditulis ke `message_archive_db` di Cassandra sebagai audit trail.

## Getting Started

```bash
git clone https://github.com/<your-org>/burung-internal-notificationing-app.git
cd burung-internal-notificationing-app

cp .env.example .env

go run ./cmd/main.go
```

## Configuration

### Environment Variables

```dotenv
# Redis
RDSHOST=localhost
RDSPORT=6379
RDSAUTHENTICATION=1        # Redis DB index untuk auth
RDSSESSION=2               # Redis DB index untuk session

# Cassandra — Message Archive DB
CASS_ARCHIVE_SPACEKEY=your_keyspace
CASS_ARCHIVE_USER=cassandra
CASS_ARCHIVE_PASS=your_password
CASS_ARCHIVE_PORT=9042
CASS_ARCHIVE_HOST=localhost

# WebSocket
WS_PORT=8080
WS_READ_BUFFER_SIZE=1024
WS_WRITE_BUFFER_SIZE=1024
WS_PING_INTERVAL=30s
WS_PONG_TIMEOUT=60s

# HTTP Server (untuk menerima request dari background worker)
HTTP_PORT=9090
HTTP_READ_TIMEOUT=10s
HTTP_WRITE_TIMEOUT=10s

# Internal secret (validasi request dari background worker)
INTERNAL_SECRET=your_internal_secret_key
```

> **Jangan commit file `.env` ke repository.** Pastikan sudah ada di `.gitignore`.

```gitignore
.env
```

## API Internal

Endpoint berikut hanya bisa diakses oleh `burung-background-worker-app` dengan header `X-Internal-Secret`.

| Method | Path | Deskripsi |
|---|---|---|
| `POST` | `/internal/notify` | Kirim notifikasi create baru |
| `PATCH` | `/internal/notify` | Kirim notifikasi update sebagian |
| `PUT` | `/internal/notify` | Kirim notifikasi update penuh |
| `DELETE` | `/internal/notify` | Kirim notifikasi penghapusan data |

