package connection_ws

import (
	"log"
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
	// Kapan koneksi harus mati? Start + 1 Jam.
	// Kita hitung sisa waktunya (durasi dari sekarang sampai waktu mati tersebut)
	expireTime := k.Start.Add(1 * time.Hour)
	remainingTime := time.Until(expireTime)

	// Jika karena suatu hal k.Start sudah lewat dari 1 jam sebelum fungsi ini jalan
	if remainingTime <= 0 {
		k.Disconnect()
		return
	}

	timer := time.NewTimer(remainingTime)
	defer timer.Stop()

	<-timer.C

	k.Disconnect()
	log.Printf("[WS] Koneksi otomatis diputus setelah 1 jam berkendara.\n")
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

func (a *ActiveConnectionsEntity) AddConnection(id int64, conn *Koneksi) {
	conn.SetTimer()
	conn.StartMonitoring()

	a.mu.Lock()
	defer a.mu.Unlock()

	panjang := len(a.KoneksiMap)
	inserted := false

	for i := 0; i < panjang; i++ {
		// Nilai maksimum per map index adalah 100
		if len(a.KoneksiMap[i]) >= 100 {
			continue
		} else {
			if a.KoneksiMap[i] == nil {
				a.KoneksiMap[i] = make(map[int64]*Koneksi)
			}
			a.KoneksiMap[i][id] = conn
			inserted = true
			break
		}
	}
	if !inserted {
		newMap := map[int64]*Koneksi{id: conn}
		a.KoneksiMap = append(a.KoneksiMap, newMap)
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

type PenggunaBaseHandshake struct {
	ID       int64  `gorm:"primaryKey;autoIncrement" json:"id_user"`
	Username string `gorm:"column:username;type:varchar(100);not null;default:''" json:"username_user"`
	Nama     string `gorm:"column:nama;type:text;not null;default:''" json:"nama_user"`
	Email    string `gorm:"column:email;type:varchar(100);not null;uniqueIndex" json:"email_user"`
}

type SellerBaseHandshake struct {
	ID       int32  `gorm:"primaryKey;autoIncrement" json:"id_seller"`
	Username string `gorm:"column:username;type:varchar(100);notnull;default:''" json:"username_seller"`
	Nama     string `gorm:"column:nama;type:varchar(150);not null;default:''" json:"nama_seller"`
	Email    string `gorm:"column:email;type:varchar(150);not null;default:''" json:"email_seller"`
}

type KurirBaseHandshake struct {
	ID       int64  `gorm:"primaryKey;autoIncrement" json:"id_kurir"`
	Nama     string `gorm:"column:nama;type:varchar(150);not null;default:''" json:"nama_kurir"`
	Username string `gorm:"column:username;type:text;not null" json:"username_kurir"`
	Email    string `gorm:"column:email;type:varchar(150);not null;default:''" json:"email_kurir"`
}
