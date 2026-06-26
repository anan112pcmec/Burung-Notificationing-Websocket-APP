package error_ws

import (
	"fmt"
	"sync"
)

type ErrorLogs struct {
	mu        sync.RWMutex
	ErrorData []error
	ErrChan   chan error
}

// Fungsi untuk inisialisasi struct (wajib bikin channel-nya dulu)
func NewErrorLogs() *ErrorLogs {
	return &ErrorLogs{
		ErrorData: make([]error, 0),
		ErrChan:   make(chan error, 100), // Buffer 100 supaya ga blocking
	}
}

func (e *ErrorLogs) AppendError(err error) {
	e.mu.Lock()
	e.ErrorData = append(e.ErrorData, err)
	e.mu.Unlock()

	// Kirim error ke channel tiap ada data masuk
	e.ErrChan <- err
}

// Fungsi ini bakal terus hidup selama program berjalan (dijadikan concurrent)
func (e *ErrorLogs) PrintError() {
	fmt.Println("[WS-LOG] Background printer untuk error telah aktif...")

	// Jauh lebih bersih! Loop ini bakal nge-block dan nungguin data masuk secara otomatis.
	// Tiap ada data masuk ke ErrChan, langsung di-println detik itu juga.
	for err := range e.ErrChan {
		fmt.Printf("[REALTIME-ERROR] %v\n", err)
	}
}
