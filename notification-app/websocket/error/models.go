package error_ws

import (
	"fmt"

)

type ErrorLogs struct {
	ErrChan chan error
}

func NewErrorLogs() ErrorLogs {
	return ErrorLogs{
		ErrChan: make(chan error, 100),
	}
}

func (e *ErrorLogs) AppendError(err error) {

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
