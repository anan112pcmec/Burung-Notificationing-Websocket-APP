package archive_migrations

import (
	"context"
	"fmt"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	cass_models "burung-notificationing-app/notification-app/database/cassandra/models"
)

func UpRelation(ctx context.Context, session *gocql.Session) []error {
	var errs []error = []error{}

	for _, model := range model_list {
		// Tetap pakai timeout 6 detik per model biar ga gantung kalau lemot
		fctx, cancel := context.WithTimeout(ctx, time.Second*6)

		if historicalModel, ok := model.(cass_models.Method); ok {
			// session langsung dipakai dari parameter fungsi
			if err := historicalModel.CreateArchiveTable(fctx, session); err != nil {
				errs = append(errs, err)
			}
		} else {
			fmt.Printf("Objek %T tidak mengimplementasikan archive_models.Method\n", model)
		}

		cancel() // Wajib di-cancel setelah selesai per iterasi biar ga leak memory
	}

	return errs
}
