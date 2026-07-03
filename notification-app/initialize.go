package notification_app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/gofiber/fiber/v3"

	"burung-notificationing-app/notification-app/bwa_http_req"
	"burung-notificationing-app/notification-app/environment"
	notification_contract "burung-notificationing-app/notification-app/notification/contract"
	notification_models "burung-notificationing-app/notification-app/notification/models"
	"burung-notificationing-app/notification-app/prevision"
	connection_models_ws "burung-notificationing-app/notification-app/websocket/connection"
	entity_handshake_ws "burung-notificationing-app/notification-app/websocket/handshake"
)

type Orchestration[T notification_models.NotificationPengguna | notification_models.NotificationKurir | notification_models.NotificationSeller] struct {
	DataBuffer  []T
	mu          sync.Mutex
	wg          sync.WaitGroup
	Timer       *time.Timer
	timerActive bool // Flag untuk menandai apakah timer sedang berjalan
	Limit       int
	Out         chan []T
}

func (O *Orchestration[T]) Insert(D T) {
	O.mu.Lock()
	defer O.mu.Unlock()

	O.DataBuffer = append(O.DataBuffer, D)
	O.Limit++

	if O.Limit < 1000 {
		if O.timerActive {
			if !O.Timer.Stop() {
				select {
				case <-O.Timer.C:
				default:
				}
			}
		}
		O.Timer.Reset(1 * time.Second)
		O.timerActive = true
		return
	}

	if O.timerActive {
		O.Timer.Stop()
	}

	select {
	case <-O.Timer.C:
	default:
	}

	O.Timer.Reset(0)
	O.timerActive = true
}

func (O *Orchestration[T]) Watch(a *connection_models_ws.ActiveConnectionsEntity, archive_db *gocql.Session) {
	for {
		<-O.Timer.C

		O.mu.Lock()
		O.timerActive = false
		if len(O.DataBuffer) == 0 {
			O.mu.Unlock()
			continue
		}

		dataSlice := O.DataBuffer
		O.DataBuffer = nil
		O.Limit = 0 // <--- WAJIB DI-RESET KE 0 DI SINI (Di dalam Lock)
		O.mu.Unlock()

		select {
		case O.Out <- dataSlice:
		default:
		}

		elemenPertama := dataSlice[0]

		// Refaktor switch: Tidak perlu cast lagi di dalam loop gofunc karena tipe 'notif' sudah konkrit setelah switch v
		switch any(elemenPertama).(type) {
		case notification_models.NotificationPengguna:
			for _, notif := range dataSlice {
				O.wg.Add(1)
				// Trik: langsung cast seluruh slice atau gunakan bayangan variabel yang tipenya sudah pasti
				go func(np notification_models.NotificationPengguna) {
					defer O.wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
					defer cancel()

					err := bwa_http_req.PenggunaNotificationInHandles(ctx, np, a, archive_db)
					if err != nil {
						environment.ErrorData.AppendError(err)
					}
				}(any(notif).(notification_models.NotificationPengguna))
			}

		case notification_models.NotificationKurir:
			for _, notif := range dataSlice {
				O.wg.Add(1)
				go func(nk notification_models.NotificationKurir) {
					defer O.wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
					defer cancel()

					err := bwa_http_req.KurirNotificationInHandles(ctx, nk, a, archive_db)
					if err != nil {
						environment.ErrorData.AppendError(err)
					}
				}(any(notif).(notification_models.NotificationKurir))
			}

		case notification_models.NotificationSeller:
			for _, notif := range dataSlice {
				O.wg.Add(1)
				go func(ns notification_models.NotificationSeller) {
					defer O.wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
					defer cancel()

					err := bwa_http_req.SellerNotificationInHandles(ctx, ns, a, archive_db)
					if err != nil {
						environment.ErrorData.AppendError(err)
					}
				}(any(notif).(notification_models.NotificationSeller))
			}
		}

		// Menunggu batch ini selesai dikirim ke WS sebelum menerima trigger timer berikutnya
		O.wg.Wait()
	}
}
func RunApp() {

	var Connection prevision.Previsioning
	Connection.InitialArchiveDB(os.Getenv("CASS_ARCHIVE_USER"), os.Getenv("CASS_ARCHIVE_PASS"), os.Getenv("CASS_ARCHIVE_PORT"), os.Getenv("CASS_ARCHIVE_SPACEKEY"))
	err_conn, archive_db := Connection.Connect()
	if err_conn != nil {
		fmt.Println("gagal koneksi", err_conn)
		return
	}
	appWsPengguna := fiber.New()
	appWsSeller := fiber.New()
	appWsKurir := fiber.New()

	appAPIInNotifikasi := fiber.New()

	var wg sync.WaitGroup

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

	var OrchestrationReqPengguna Orchestration[notification_models.NotificationPengguna] = Orchestration[notification_models.NotificationPengguna]{
		Timer: time.NewTimer(3 * time.Second),
		Out:   make(chan []notification_models.NotificationPengguna),
	}
	OrchestrationReqPengguna.Timer.Stop()
	go OrchestrationReqPengguna.Watch(&environment.EntityPenggunaActive, archive_db)

	var OrchestrationReqSeller Orchestration[notification_models.NotificationSeller]
	OrchestrationReqSeller = Orchestration[notification_models.NotificationSeller]{
		Timer: time.NewTimer(3 * time.Second),
		Out:   make(chan []notification_models.NotificationSeller),
	}
	OrchestrationReqSeller.Timer.Stop()
	go OrchestrationReqSeller.Watch(&environment.EntitySellerActive, archive_db)

	var OrchestrationReqKurir Orchestration[notification_models.NotificationKurir]
	OrchestrationReqKurir = Orchestration[notification_models.NotificationKurir]{
		Timer: time.NewTimer(3 * time.Second),
		Out:   make(chan []notification_models.NotificationKurir),
	}
	OrchestrationReqKurir.Timer.Stop()
	go OrchestrationReqKurir.Watch(&environment.EntityKurirActive, archive_db)

	// 4. Goroutine - HTTP API Notifikasi Masuk
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Mendaftarkan 3 path POST berbeda untuk Notifikasi Masuk
		appAPIInNotifikasi.Post(environment.PenggunaPathNotifikasiMasuk, func(c fiber.Ctx) error {

			data := c.Body()
			err, parsedData := notification_contract.NotificationManager[notification_models.NotificationPengguna]{}.ParseNotification(data)
			if err != nil {
				return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"message": "Data Tidak Sesuai"})
			}

			OrchestrationReqPengguna.Insert(parsedData)
			return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "Notifikasi Pengguna diterima"})

			// Logika ketika notifikasi pengguna masuk

		})

		appAPIInNotifikasi.Post(environment.SellerPathNotifikasiMasuk, func(c fiber.Ctx) error {

			data := c.Body()
			err, parsedData := notification_contract.NotificationManager[notification_models.NotificationSeller]{}.ParseNotification(data)
			if err != nil {
				return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"message": "Data Tidak Sesuai"})
			}

			OrchestrationReqSeller.Insert(parsedData)
			// Logika ketika notifikasi seller masuk
			return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "Notifikasi Seller diterima"})
		})

		appAPIInNotifikasi.Post(environment.KurirPathNotifikasiMasuk, func(c fiber.Ctx) error {

			data := c.Body()
			err, parsedData := notification_contract.NotificationManager[notification_models.NotificationKurir]{}.ParseNotification(data)
			if err != nil {
				return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"message": "Data Tidak Sesuai"})
			}

			OrchestrationReqKurir.Insert(parsedData)
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
