package notification_app

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/gofiber/fiber/v3"

	"burung-notificationing-app/notification-app/environment"
	entity_handshake_ws "burung-notificationing-app/notification-app/websocket/handshake"
)

func RunApp() {
	// Inisialisasi fiber
	appWsPengguna := fiber.New()
	appWsSeller := fiber.New()
	appWsKurir := fiber.New()

	// Instance untuk HTTP Request biasa (Notifikasi Masuk)
	appAPIInNotifikasi := fiber.New()

	var wg sync.WaitGroup

	// 1. Goroutine - WS Pengguna
	wg.Add(1)
	go func() {
		defer wg.Done()
		entity_handshake_ws.HandshakePengguna(environment.PenggunaPathHandShake, &environment.EntityPenggunaActive, appWsPengguna, &environment.ErrorData)

		fmt.Println("Memulai listen ke port ws pengguna...")
		if err := appWsPengguna.Listen(fmt.Sprintf(":%s", environment.PortRunningWSAppPengguna)); err != nil {
			environment.ErrorData.AppendError(err)
		}
	}()

	// 2. Goroutine - WS Seller
	wg.Add(1)
	go func() {
		defer wg.Done()
		entity_handshake_ws.HandshakeSeller(environment.SellerPathHandShake, &environment.EntitySellerActive, appWsSeller, &environment.ErrorData)

		fmt.Println("Memulai listen ke port ws seller...")
		if err := appWsSeller.Listen(fmt.Sprintf(":%s", environment.PortRunningWSAppSeller)); err != nil {
			environment.ErrorData.AppendError(err)
		}
	}()

	// 3. Goroutine - WS Kurir
	wg.Add(1)
	go func() {
		defer wg.Done()
		entity_handshake_ws.HandshakeKurir(environment.KurirPathHandShake, &environment.EntityKurirActive, appWsKurir, &environment.ErrorData)

		fmt.Println("Memulai listen ke port ws kurir...")
		if err := appWsKurir.Listen(fmt.Sprintf(":%s", environment.PortRunningWSAppKurir)); err != nil {
			environment.ErrorData.AppendError(err)
		}
	}()

	// 4. Goroutine - HTTP API Notifikasi Masuk
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Mendaftarkan 3 path POST berbeda untuk Notifikasi Masuk
		appAPIInNotifikasi.Post(environment.PenggunaPathNotifikasiMasuk, func(c fiber.Ctx) error {
			// Logika ketika notifikasi pengguna masuk
			return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "Notifikasi Pengguna diterima"})
		})

		appAPIInNotifikasi.Post(environment.SellerPathNotifikasiMasuk, func(c fiber.Ctx) error {
			// Logika ketika notifikasi seller masuk
			return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "Notifikasi Seller diterima"})
		})

		appAPIInNotifikasi.Post(environment.KurirPathNotifikasiMasuk, func(c fiber.Ctx) error {
			// Logika ketika notifikasi kurir masuk
			return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "Notifikasi Kurir diterima"})
		})

		fmt.Println("Memulai listen ke port API In Notifikasi...")
		// Pastikan variabel port ini sudah ada di struct environment kamu, misal: PortRunningAPIInNotifikasi
		if err := appAPIInNotifikasi.Listen(fmt.Sprintf(":%s", environment.PortRunningAPIInNotifikasi)); err != nil {
			environment.ErrorData.AppendError(err)
		}
	}()

	// Menahan main thread sampai ada signal shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig

	fmt.Println("Sistem mendeteksi shutdown, mematikan semua fiber instance...")

	// Mematikan seluruh instance secara Graceful
	_ = appWsPengguna.Shutdown()
	_ = appWsSeller.Shutdown()
	_ = appWsKurir.Shutdown()
	_ = appAPIInNotifikasi.Shutdown() // Ikut dimatikan dengan aman

	wg.Wait()
	fmt.Println("sistem notificationing berhenti total")
}
