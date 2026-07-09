package notification_app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/joho/godotenv"

	"burung-notificationing-app/notification-app/bwa_http_req"
	"burung-notificationing-app/notification-app/cache"
	archive_migrations "burung-notificationing-app/notification-app/database/cassandra/archive_db/migrations"
	"burung-notificationing-app/notification-app/environment"
	notification_contract "burung-notificationing-app/notification-app/notification/contract"
	notification_models "burung-notificationing-app/notification-app/notification/models"
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
						cache.ErrorData.AppendError(err)
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
						cache.ErrorData.AppendError(err)
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
						cache.ErrorData.AppendError(err)
					}
				}(any(notif).(notification_models.NotificationSeller))
			}
		}

		O.wg.Wait()
	}
}
func RunApp(rebootcass bool) {

	go cache.ErrorData.PrintError()
	time.Sleep(time.Second * 2)
	// Mengarah langsung ke file .env di folder yang sama
	if err := godotenv.Load("C:/Burung_App/Project_Source/Backend-3/.env"); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	fmt.Println("mencoba memulai websocket app")

	var Connection environment.Environment
	Connection.InitialArchiveDB(
		os.Getenv("CASS_ARCHIVE_USER"),     // USER (.env: cassandra)
		os.Getenv("CASS_ARCHIVE_PASS"),     // PASS (.env: cassandra)
		os.Getenv("CASS_ARCHIVE_PORT"),     // PORT (.env: 9042)
		os.Getenv("CASS_ARCHIVE_SPACEKEY"), // SPACEKEY (.env: archive_db)
	)

	rds_db, err := strconv.Atoi(os.Getenv("RDS_SESSION"))
	if err != nil {
		rds_db = 1 // Kalau di .env kosong atau typo, otomatis pake database 1
	}

	Connection.InitialSessionCache(
		os.Getenv("RDS_HOST"), // HOST (.env: localhost)
		os.Getenv("RDS_PORT"), // PORT (.env: 6379)
		rds_db,                // SESSION DB (Udah jadi int dari hasil convert di atas)
	)
	err_conn, archive_db, redis_session := Connection.Connect()
	if err_conn != nil {
		fmt.Println("gagal koneksi", err_conn)
		return
	}
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		cache.ErrorData.AppendError(errors.New("nyoba tes sajo"))
	}

	// 1. Jalankan DownRelation
	if rebootcass {
		if errs := archive_migrations.DownRelation(ctx, archive_db); len(errs) > 0 && errs[0] != nil {
			cache.ErrorData.AppendError(err_conn)
		}

	}
	// 2. Jalankan UpRelation
	if errs := archive_migrations.UpRelation(ctx, archive_db); len(errs) > 0 && errs[0] != nil {
		cache.ErrorData.AppendError(errs[0])
	}
	appWsPengguna := fiber.New()
	appWsSeller := fiber.New()
	appWsKurir := fiber.New()

	appAPIInNotifikasi := fiber.New()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Print("coba ngehubungin ws pengguna")
		entity_handshake_ws.HandshakePengguna(cache.PenggunaPathHandShake, &cache.EntityPenggunaActive, appWsPengguna, &cache.ErrorData, redis_session)

		fmt.Println("Memulai listen ke port ws pengguna...")
		if err := appWsPengguna.Listen(fmt.Sprintf(":%s", cache.PortRunningWSAppPengguna)); err != nil {
			cache.ErrorData.AppendError(err)
		}
	}()

	// 2. Goroutine - WS Seller
	wg.Add(1)
	go func() {
		defer wg.Done()
		entity_handshake_ws.HandshakeSeller(cache.SellerPathHandShake, &cache.EntitySellerActive, appWsSeller, &cache.ErrorData, redis_session)

		fmt.Println("Memulai listen ke port ws seller...")
		if err := appWsSeller.Listen(fmt.Sprintf(":%s", cache.PortRunningWSAppSeller)); err != nil {
			cache.ErrorData.AppendError(err)
		}
	}()

	// 3. Goroutine - WS Kurir
	wg.Add(1)
	go func() {
		defer wg.Done()
		entity_handshake_ws.HandshakeKurir(cache.KurirPathHandShake, &cache.EntityKurirActive, appWsKurir, &cache.ErrorData, redis_session)

		fmt.Println("Memulai listen ke port ws kurir...")
		if err := appWsKurir.Listen(fmt.Sprintf(":%s", cache.PortRunningWSAppKurir)); err != nil {
			cache.ErrorData.AppendError(err)
		}
	}()

	var OrchestrationReqPengguna Orchestration[notification_models.NotificationPengguna] = Orchestration[notification_models.NotificationPengguna]{
		Timer: time.NewTimer(3 * time.Second),
		Out:   make(chan []notification_models.NotificationPengguna),
	}
	OrchestrationReqPengguna.Timer.Stop()
	go OrchestrationReqPengguna.Watch(&cache.EntityPenggunaActive, archive_db)

	var OrchestrationReqSeller Orchestration[notification_models.NotificationSeller]
	OrchestrationReqSeller = Orchestration[notification_models.NotificationSeller]{
		Timer: time.NewTimer(3 * time.Second),
		Out:   make(chan []notification_models.NotificationSeller),
	}
	OrchestrationReqSeller.Timer.Stop()
	go OrchestrationReqSeller.Watch(&cache.EntitySellerActive, archive_db)

	var OrchestrationReqKurir Orchestration[notification_models.NotificationKurir]
	OrchestrationReqKurir = Orchestration[notification_models.NotificationKurir]{
		Timer: time.NewTimer(3 * time.Second),
		Out:   make(chan []notification_models.NotificationKurir),
	}
	OrchestrationReqKurir.Timer.Stop()
	go OrchestrationReqKurir.Watch(&cache.EntityKurirActive, archive_db)

	// 4. Goroutine - HTTP API Notifikasi Masuk
	wg.Add(1)
	go func() {
		defer wg.Done()

		// 1. Path POST untuk Notifikasi Pengguna Masuk
		appAPIInNotifikasi.Post(cache.PenggunaPathNotifikasiMasuk, func(c *fiber.Ctx) error {
			data := c.Body()
			err, parsedData := notification_contract.NotificationManager[notification_models.NotificationPengguna]{}.ParseNotification(data)
			if err != nil {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
			}

			OrchestrationReqPengguna.Insert(parsedData)

			return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "Notifikasi pengguna berhasil diproses"})
		})

		// 2. Path POST untuk Notifikasi Seller Masuk
		appAPIInNotifikasi.Post(cache.SellerPathNotifikasiMasuk, func(c *fiber.Ctx) error {
			data := c.Body()
			err, parsedData := notification_contract.NotificationManager[notification_models.NotificationSeller]{}.ParseNotification(data)
			if err != nil {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
			}

			OrchestrationReqSeller.Insert(parsedData)

			return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "Notifikasi seller berhasil diproses"})
		})

		appAPIInNotifikasi.Post(cache.KurirPathNotifikasiMasuk, func(c *fiber.Ctx) error {
			data := c.Body()
			err, parsedData := notification_contract.NotificationManager[notification_models.NotificationKurir]{}.ParseNotification(data)
			if err != nil {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
			}

			OrchestrationReqKurir.Insert(parsedData)

			return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "Notifikasi kurir berhasil diproses"})
		})

		fmt.Println("Memulai listen ke port API In Notifikasi...")
		if err := appAPIInNotifikasi.Listen(fmt.Sprintf(":%s", cache.PortRunningAPIInNotifikasi)); err != nil {
			cache.ErrorData.AppendError(err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig

	fmt.Println("Sistem mendeteksi shutdown, mematikan semua fiber instance...")

	_ = appWsPengguna.Shutdown()
	_ = appWsSeller.Shutdown()
	_ = appWsKurir.Shutdown()
	_ = appAPIInNotifikasi.Shutdown()

	wg.Wait()
	fmt.Println("sistem notificationing berhenti total")
}
