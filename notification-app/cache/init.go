package cache

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

func init() {

	if err := godotenv.Load("C:/Burung_App/Project_Source/Backend-3/.env"); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	// 1. Path Handshake (Websocket)
	PenggunaPathHandShake = os.Getenv("PENGGUNA_PATH_HANDSHAKE")
	SellerPathHandShake = os.Getenv("SELLER_PATH_HANDSHAKE")
	KurirPathHandShake = os.Getenv("KURIR_PATH_HANDSHAKE")

	// 2. Path Notifikasi Masuk (API HTTP)
	PenggunaPathNotifikasiMasuk = os.Getenv("PENGGUNA_PATH_NOTIFIKASI_MASUK")
	SellerPathNotifikasiMasuk = os.Getenv("SELLER_PATH_NOTIFIKASI_MASUK") // Sudah di-fix dari typo sebelumnya
	KurirPathNotifikasiMasuk = os.Getenv("KURIR_PATH_NOTIFIKASI_MASUK")   // Sudah di-fix dari typo sebelumnya

	// 3. Port API In Notifikasi
	PortRunningAPIInNotifikasi = os.Getenv("API_IN_NOTIFIKASI_PORT")

	// 4. Port Websocket
	PortRunningWSAppPengguna = os.Getenv("WEBSOCKET_PORT_PENGGUNA")
	PortRunningWSAppSeller = os.Getenv("WEBSOCKET_PORT_SELLER")
	PortRunningWSAppKurir = os.Getenv("WEBSOCKET_PORT_KURIR")
}
