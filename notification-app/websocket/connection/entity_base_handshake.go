package connection_models_ws

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/gofiber/contrib/websocket"

)

type Koneksi struct {
	Connected bool
	Start     time.Time
	Conn      *websocket.Conn
}

func (k *Koneksi) SetTimer() {
	k.Connected = true
	k.Start = time.Now()
}

func (k *Koneksi) StartMonitoring() {
	fmt.Println("========== START MONITORING ==========")

	fmt.Printf("Connected : %v\n", k.Connected)
	fmt.Printf("Start     : %v\n", k.Start)
	fmt.Printf("Now       : %v\n", time.Now())

	expire := k.Start.Add(time.Hour)

	fmt.Printf("Expire    : %v\n", expire)

	remaining := time.Until(expire)

	fmt.Printf("Remaining : %v\n", remaining)

	if remaining <= 0 {
		fmt.Println("EXPIRED")
		k.Disconnect()
		return
	}

	fmt.Println("Timer dibuat")

	timer := time.NewTimer(remaining)

	<-timer.C

	fmt.Println("Timer selesai")

	k.Disconnect()
}

// Helper method untuk merapikan proses disconnect
func (k *Koneksi) Disconnect() {
	if k.Conn != nil {
		k.Conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Session expired (1 hour limit)"))
		k.Conn.Close()
	}

	k.Connected = false
}

type ActiveConnectionsEntity struct {
	mu         sync.RWMutex
	KoneksiMap []map[int64]*Koneksi
}

type Target struct {
	bagan int
	id    int64
}

func PenentuBagan(angka int64) int64 {
	if angka <= 100 {
		return 1
	}

	ratusanKeAtas := math.Ceil(float64(angka) / 100.0) // 104 / 100 = 1.04 -> Ceil = 2

	output := math.Ceil(ratusanKeAtas/10.0) * 10

	return int64(output)
}

func (a *ActiveConnectionsEntity) SendNotificationDirect(data interface{}, idPenerima int64) {

	fmt.Println("========== SEND NOTIFICATION DIRECT ==========")

	fmt.Printf("[INPUT] idPenerima=%d\n", idPenerima)
	fmt.Printf("[INPUT] payload=%+v\n", data)

	indexBagan := PenentuBagan(idPenerima) - 1

	fmt.Printf("[STEP 1] penentuBagan(%d) -> %d\n", idPenerima, indexBagan)

	a.mu.RLock()
	defer func() {
		a.mu.RUnlock()
		fmt.Println("========== END SEND NOTIFICATION DIRECT ==========")
	}()

	fmt.Printf("[STEP 2] Total shard/map = %d\n", len(a.KoneksiMap))

	if indexBagan < 0 || indexBagan >= int64(len(a.KoneksiMap)) {
		fmt.Printf("[FAILED] indexBagan=%d berada di luar range map\n", indexBagan)
		return
	}

	fmt.Println("[SUCCESS] indexBagan valid")

	targetMap := a.KoneksiMap[indexBagan]

	if targetMap == nil {
		fmt.Printf("[FAILED] shard/map pada index %d = nil\n", indexBagan)
		return
	}

	fmt.Printf("[SUCCESS] shard ditemukan. Jumlah koneksi dalam shard = %d\n", len(targetMap))

	fmt.Println("[STEP 3] Lookup koneksi berdasarkan idPenerima")

	koneksiUser, ada := targetMap[idPenerima]

	if !ada {
		fmt.Printf("[FAILED] idPenerima=%d tidak ditemukan dalam shard\n", idPenerima)

		fmt.Println("[DEBUG] Daftar key yang tersedia:")

		for id := range targetMap {
			fmt.Printf("  -> %d\n", id)
		}

		return
	}

	fmt.Printf("[SUCCESS] Koneksi ditemukan untuk ID=%d\n", idPenerima)

	if koneksiUser == nil {
		fmt.Println("[FAILED] koneksiUser == nil")
		return
	}

	if koneksiUser.Conn == nil {
		fmt.Println("[FAILED] koneksiUser.Conn == nil")
		return
	}

	fmt.Println("[STEP 4] Mengirim payload via websocket")

	if err := koneksiUser.Conn.WriteJSON(data); err != nil {
		fmt.Printf("[FAILED] WriteJSON error: %v\n", err)
		return
	}

	fmt.Printf("[SUCCESS] Payload berhasil dikirim ke ID=%d\n", idPenerima)
}
func (a *ActiveConnectionsEntity) SendNotificationBroadCast(data interface{}, idsPenerima ...int64) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	for _, idnye := range idsPenerima {
		bagan := PenentuBagan(idnye)

		if bagan < 0 || bagan >= int64(len(a.KoneksiMap)) {
			continue
		}

		if targetMap := a.KoneksiMap[bagan]; targetMap != nil {
			if koneksiUser, ada := targetMap[idnye]; ada && koneksiUser.Conn != nil {

				go func(conn *websocket.Conn) {
					_ = conn.WriteJSON(data)
				}(koneksiUser.Conn)

			}
		}
	}
}
func (a *ActiveConnectionsEntity) AddConnection(id int64, conn *Koneksi) {
	fmt.Println("======================================================")
	fmt.Println("            ADD CONNECTION")
	fmt.Println("======================================================")

	fmt.Printf("[INFO] Incoming Connection ID : %d\n", id)

	conn.SetTimer()
	fmt.Println("[SUCCESS] Timer initialized")

	go conn.StartMonitoring()
	fmt.Println("[SUCCESS] Monitoring started")

	a.mu.Lock()
	defer func() {
		fmt.Println("[INFO] Unlock ActiveConnections")
		a.mu.Unlock()
		fmt.Println("======================================================")
		fmt.Println("          END ADD CONNECTION")
		fmt.Println("======================================================")
	}()

	fmt.Println("[INFO] ActiveConnections locked")

	panjang := len(a.KoneksiMap)
	inserted := false

	fmt.Printf("[INFO] Total bagan saat ini : %d\n", panjang)

	for i := 0; i < panjang; i++ {

		fmt.Println("----------------------------------------------")
		fmt.Printf("[CHECK] Bagan Index : %d\n", i)

		if a.KoneksiMap[i] == nil {
			fmt.Println("[INFO] Map pada bagan ini = NIL")
		} else {
			fmt.Printf("[INFO] Jumlah koneksi pada bagan : %d\n", len(a.KoneksiMap[i]))
		}

		// Nilai maksimum per map index adalah 100
		if len(a.KoneksiMap[i]) >= 100 {
			fmt.Printf("[SKIP] Bagan %d penuh (%d/100)\n", i, len(a.KoneksiMap[i]))
			continue
		}

		fmt.Printf("[SUCCESS] Bagan %d masih tersedia\n", i)

		if a.KoneksiMap[i] == nil {
			fmt.Printf("[ACTION] Membuat map baru pada bagan %d\n", i)
			a.KoneksiMap[i] = make(map[int64]*Koneksi)
		}

		fmt.Printf("[ACTION] Menyimpan ID %d ke bagan %d\n", id, i)

		a.KoneksiMap[i][id] = conn

		fmt.Printf("[SUCCESS] ID %d berhasil disimpan pada bagan %d\n", id, i)
		fmt.Printf("[INFO] Jumlah koneksi bagan %d sekarang : %d\n", i, len(a.KoneksiMap[i]))

		inserted = true
		break
	}

	if !inserted {
		fmt.Println("----------------------------------------------")
		fmt.Println("[INFO] Semua bagan penuh")

		newMap := map[int64]*Koneksi{
			id: conn,
		}

		a.KoneksiMap = append(a.KoneksiMap, newMap)

		fmt.Printf("[SUCCESS] Membuat bagan BARU index %d\n", len(a.KoneksiMap)-1)
		fmt.Printf("[SUCCESS] ID %d dimasukkan ke bagan baru\n", id)
	}

	fmt.Println("----------------------------------------------")
	fmt.Println("[SUMMARY]")
	fmt.Printf("Total Bagan : %d\n", len(a.KoneksiMap))

	for idx, mp := range a.KoneksiMap {
		if mp == nil {
			fmt.Printf("Bagan[%d] -> NIL\n", idx)
			continue
		}

		fmt.Printf("Bagan[%d] -> %d koneksi\n", idx, len(mp))

		for key := range mp {
			fmt.Printf("    └── ID : %d\n", key)
		}
	}
}

func (a *ActiveConnectionsEntity) SearchForCleanUp() {
	a.mu.RLock() // Lock dulu sebelum baca slice & map
	panjang := len(a.KoneksiMap)

	var targets []Target

	for i := 0; i < panjang; i++ {
		for id, val := range a.KoneksiMap[i] {
			if val.Conn == nil && !val.Connected {
				targets = append(targets, Target{bagan: i, id: id})
			}
		}
	}
	a.mu.RUnlock()

	if len(targets) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func(baganIndex int, koneksiID int64) {
			defer wg.Done()
			// Panggil fungsi pembersih yang pakai Write Lock
			a.CleanUpClient(baganIndex, koneksiID)
		}(t.bagan, t.id)
	}

	wg.Wait()
}

// Parameter dikurangi 1 (dari bagan1 & bagan2 menjadi bagan saja) karena array turun 1 tingkat
func (a *ActiveConnectionsEntity) CleanUpClient(bagan int, id int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if bagan < len(a.KoneksiMap) && a.KoneksiMap[bagan] != nil {
		delete(a.KoneksiMap[bagan], id)
	}
}
